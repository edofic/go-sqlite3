#ifndef _WASM_SIMD128_STDLIB_H
#define _WASM_SIMD128_STDLIB_H

#include <stddef.h>
#include <stdint.h>

#include_next <stdlib.h>  // the system stdlib.h

#ifdef __cplusplus
extern "C" {
#endif

// Shellsort with Gonnet & Baeza-Yates gap sequence.
// Simple, no recursion, doesn't use the C stack.
// Clang auto-vectorizes the inner loop.

__attribute__((weak))
void qsort(void *base, size_t nel, size_t width,
           int (*comp)(const void *, const void *)) {
  // If nel is zero or one, there's nothing to do.
  size_t wnel = width * nel;
  size_t gap = nel;
  while (gap > 1) {
    // Use 64-bit unsigned arithmetic to avoid intermediate overflow.
    // Absent overflow, gap will be strictly less than its previous value.
    // Once it is one or zero, set it to one: do a final pass, and stop.
    gap = (5ull * gap - 1) / 11;
    if (gap == 0) gap = 1;

    // It's undefined behavior if wgap or wnel overflows:
    // the array would be larger than (or as large as) memory;
    // or if width is zero: the base pointer would be invalid.
    // Since gap is stricly less than nel,
    // assume wgap is strictly less than wnel.
    // This skips an unnecessary check.
    size_t wgap = width * gap;
    __builtin_assume(wgap < wnel);
    for (size_t i = wgap; i < wnel; i += width) {
      // Using the overflow builtin helps the compiler,
      // even though Wasm has no overflow flags.
      for (size_t j = i; !__builtin_sub_overflow(j, wgap, &j);) {
        char *a = j + (char *)base;
        char *b = a + wgap;
        if (comp(a, b) <= 0) break;

        // This well known loop is vectorized.
        size_t s = width;
        do {
          char tmp = *a;
          *a++ = *b;
          *b++ = tmp;
        } while (--s);
      }
    }
  }
}

#ifdef __cplusplus
}  // extern "C"
#endif

#endif  // _WASM_SIMD128_STDLIB_H