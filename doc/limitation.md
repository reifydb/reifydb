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


## max literal
max accepted literal = max uint16 = 340282366920938463463374607431768211455
uint16 is max supported type at the moment, therefore the type can not be widened and will always fails with an error message
- this limitation might be removed when introducing a decimal and or bigint type

## min literal
max int16 -170141183460469231731687303715884105728 for the same reason

## float 8
float literal -  -1.8e308 to +1.8e308 for the same reason

# casting signed/unsigned to float  
max 2^24 = 16,777,216  for float4  - otherwise loss of precision
max 2^53 = 9,007,199,254,740,992 for float8  - otherwise loss of precision
