#if !NET5_0_OR_GREATER
namespace System.Runtime.CompilerServices;

// Enables records and init-only setters on netstandard2.1.
internal static class IsExternalInit
{
}
#endif
