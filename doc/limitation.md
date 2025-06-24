# Limitation

- comparison of numeric values for huge values, like i128 > u128, f64 == i128 - can be fixed when introducing big decimal type

- float4, float8 - no == or !=
rational:
- 
1. IEEE-754 Semantics Are Inconsistent

   Floating-point equality is notoriously fragile:

   let a = 0.1f64 + 0.2f64;
   let b = 0.3f64;
   println!("{}", a == b); // false

2. Encourages Safer Alternatives

   For domain correctness, users should explicitly choose:

        abs(a - b) < ε for tolerant equality
        2. 

use between 0.999 and 1.001 instead