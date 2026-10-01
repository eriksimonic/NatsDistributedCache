#!/usr/bin/env python3
"""Sabotage checks: prove the unit tests catch deliberate violations of the design's rules.

Each entry in sabotage.json breaks one rule in a copy of the repo (find/replace, optionally a second
insert), rebuilds, runs the unit tests and records which tests failed. A check passes (CAUGHT) when at
least one of its expected tests fails. MISSED = every test passed with the bug in place (a test gap);
OTHER = tests failed, but none of the expected ones; BROKEN = the sabotage did not compile or did not
apply. The working tree is never touched.

    tests/Sabotage/run.py [--only id,id] [--jobs N] [--report tests/Sabotage/REPORT.md]
"""
import argparse, json, os, queue, re, shutil, subprocess, sys, tempfile, time
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
TESTS = "tests/NatsDistributedCache.UnitTests"
FAIL = re.compile(r"NatsDistributedCache\.UnitTests\.\w+\.(\w+)(?:\(.*?\))? \[FAIL\]")
TOTAL = re.compile(r"Total: (\d+), Errors: (\d+), Failed: (\d+)")


def sh(cmd, cwd, timeout):
    try:
        p = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True, timeout=timeout)
        return p.returncode, p.stdout + p.stderr
    except subprocess.TimeoutExpired as e:
        return None, (e.stdout or b"").decode() if isinstance(e.stdout, bytes) else (e.stdout or "")


def build(work):
    code, out = sh(["dotnet", "build", TESTS, "-v", "q", "-nologo"], work, 600)
    return code == 0, out


def run_tests(work):
    code, out = sh(["dotnet", "run", "--no-build", "--project", TESTS], work, 300)
    failed = sorted(set(FAIL.findall(out)))
    total = TOTAL.search(out)
    return code is None, failed, total.group(0) if total else "no summary", out


def apply(work, s):
    path = os.path.join(work, s["file"])
    src = open(path, encoding="utf-8").read()
    if src.count(s["find"]) != 1:
        return f"find text occurs {src.count(s['find'])} times"
    src = src.replace(s["find"], s["replace"])
    if "append_after" in s:
        if src.count(s["append_after"]) != 1:
            return f"append_after text occurs {src.count(s['append_after'])} times"
        src = src.replace(s["append_after"], s["append_after"] + s["append"])
    open(path, "w", encoding="utf-8").write(src)
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", default="")
    ap.add_argument("--jobs", type=int, default=1, help="checks run in parallel, each in its own copy of the repo")
    ap.add_argument("--report", default=os.path.join(ROOT, "tests", "Sabotage", "REPORT.md"))
    args = ap.parse_args()
    checks = json.load(open(os.path.join(os.path.dirname(__file__), "sabotage.json"), encoding="utf-8"))
    if args.only:
        wanted = set(args.only.split(","))
        checks = [c for c in checks if c["id"] in wanted]

    copies = []
    try:
        for _ in range(max(1, args.jobs)):
            work = tempfile.mkdtemp(prefix="sabotage-")
            copies.append(work)
            subprocess.run(["rsync", "-a", "--exclude", "bin/", "--exclude", "obj/", "--exclude", ".git/",
                            "--exclude", "spike/results/", "--exclude", "TestResults/", "--exclude", "StrykerOutput/",
                            ROOT + "/", work + "/"], check=True)
        pristine = {c["file"]: open(os.path.join(copies[0], c["file"]), encoding="utf-8").read() for c in checks}

        ok, out = build(copies[0])
        if not ok:
            sys.exit("baseline build failed:\n" + out[-3000:])
        timed_out, failed, baseline, _ = run_tests(copies[0])
        print(f"baseline: {baseline}", flush=True)
        if timed_out or failed:
            sys.exit(f"baseline must be green; failed: {failed}")

        free = queue.Queue()
        for w in copies:
            free.put(w)

        def check(c):
            work = free.get()
            try:
                for f, text in pristine.items():
                    open(os.path.join(work, f), "w", encoding="utf-8").write(text)
                started = time.time()
                err = apply(work, c)
                if err:
                    verdict, failed, total = "BROKEN", [], err
                else:
                    ok, out = build(work)
                    if not ok:
                        verdict, failed, total = "BROKEN", [], "did not compile: " + " ".join(re.findall(r"error \w+: [^\[]+", out)[:2])
                    else:
                        timed_out, failed, total, _ = run_tests(work)
                        if timed_out:
                            verdict, total = "CAUGHT", "test run hung (timeout 300 s)"
                        elif set(failed) & set(c["expect"]):
                            verdict = "CAUGHT"
                        elif failed:
                            verdict = "OTHER"
                        else:
                            verdict = "MISSED"
                print(f"{verdict:7} {c['id']}  ({total}; {len(failed)} failed)", flush=True)
                return {**c, "verdict": verdict, "failed": failed, "summary": total, "secs": round(time.time() - started)}
            finally:
                free.put(work)

        with ThreadPoolExecutor(max_workers=len(copies)) as pool:
            rows = list(pool.map(check, checks))  # report keeps the manifest order
    finally:
        for w in copies:
            shutil.rmtree(w, ignore_errors=True)

    counts = {v: sum(r["verdict"] == v for r in rows) for v in ("CAUGHT", "OTHER", "MISSED", "BROKEN")}
    with open(args.report, "w", encoding="utf-8") as f:
        f.write("# Sabotage report\n\n")
        f.write(f"Generated {time.strftime('%Y-%m-%d %H:%M')} by `tests/Sabotage/run.py`. Baseline: {baseline}.\n\n")
        f.write(" · ".join(f"{k} {v}" for k, v in counts.items()) + f" of {len(rows)}\n\n")
        f.write("| Verdict | Check | Rule broken | Expected test | Tests that failed |\n| --- | --- | --- | --- | --- |\n")
        for r in rows:
            failed = ", ".join(f"`{t}`" for t in r["failed"][:6]) + (f" (+{len(r['failed']) - 6})" if len(r["failed"]) > 6 else "")
            f.write(f"| {r['verdict']} | `{r['id']}` | {r['rule']} | {', '.join('`'+e+'`' for e in r['expect'])} | {failed or r['summary']} |\n")
    print(f"report: {args.report}")
    sys.exit(0 if counts["MISSED"] == counts["BROKEN"] == 0 else 1)


if __name__ == "__main__":
    main()
