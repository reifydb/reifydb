macro_rules! validate_loop {
    (@impl $($loop:tt)*) => {
        let mut c_values_hit = Vec::new();
        const_for::const_for!(i in $($loop)* => {
            c_values_hit.push(i);
        });

        let mut r_values_hit = Vec::new();
        for i in $($loop)* {
            r_values_hit.push(i);
        };

        assert!(c_values_hit == r_values_hit);
    };

    ($step: expr, $($loop:tt)*) => {
        validate_loop!(@impl ($($loop)*).step_by($step));
        validate_loop!(@impl ($($loop)*).step_by($step).rev());
        validate_loop!(@impl ($($loop)*).rev().step_by($step));
    };

    ($($loop:tt)*) => {
        validate_loop!(@impl $($loop)*);
        validate_loop!(@impl ($($loop)*).rev());

        validate_loop!(1, $($loop)*);
        validate_loop!(2, $($loop)*);
        validate_loop!(3, $($loop)*);
        validate_loop!(8, $($loop)*);
        validate_loop!(15, $($loop)*);
        validate_loop!(17, $($loop)*);
        validate_loop!(45, $($loop)*);
        validate_loop!(150, $($loop)*);
    };
}

#[allow(unused_parens)]
#[test]
fn equivalent_to_regular_for() {
    validate_loop!(-10..10);
    validate_loop!(0..10);
    validate_loop!((0..10));
    validate_loop!(100..10);
    validate_loop!(-15..-12);
    validate_loop!(-14..0);
    validate_loop!(-100..-50);
    validate_loop!(-14..200);
    validate_loop!(1..11110);
}

#[test]
fn capture_range_at_beginning() {
    let mut a = 113;
    const_for::const_for!(i in 0..a-100 => {
        a += i;
    });
    let mut b = 113;
    for i in 0..b - 100 {
        b += i;
    }
    assert_eq!(a, b);

    let mut a = 0;
    let mut step = 1;
    const_for::const_for!(_ in (0..10).step_by(step) => {
        a += step;
        step += 1;
    });
    let mut b = 0;
    let mut step = 1;
    for _ in (0..10).step_by(step) {
        b += step;
        step += 1;
    }
    assert_eq!(a, b);
}

#[test]
const fn available_in_const() {
    let mut a = 0;

    const_for::const_for!(_ in 0..25 => {
        a += 1
    });
    const_for::const_for!(_ in (0..25).rev() => {
        a += 1
    });
    const_for::const_for!(_ in (0..100).step_by(2) =>
        a += 1
    );

    const_for::const_for!(mut i in 0..3 => {
        i += 1;
        a += i
    });

    const_for::const_for!(_ in (0..7).rev() => {
        a += 1
    });

    // (0..10).step_by(3).rev() => 9, 6, 3, 0  (4 values)
    const_for::const_for!(_ in (0..10).step_by(3).rev() => {
        a += 1
    });

    // (0..10).rev().step_by(3) => 9, 6, 3, 0  (4 values)
    const_for::const_for!(_ in (0..10).rev().step_by(3) => {
        a += 1
    });

    assert!(a == 25 + 25 + 50 + 6 + 7 + 4 + 4);
}

#[test]
fn break_and_continue() {
    let mut hits = Vec::new();
    const_for::const_for!(i in 0..10 => {
        if i % 2 == 0 { continue }
        if i == 7 { break }
        hits.push(i);
    });
    assert_eq!(hits, vec![1, 3, 5]);
}

#[test]
fn no_underflow() {
    const_for::const_for!(_ in (0u64..1).rev() => {});
    let mut iterations: u64 = 0;
    const_for::const_for!(_ in (i8::MIN..0).rev() => iterations += 1);
    assert_eq!(iterations, 128);
}

#[test]
fn signed_can_go_negative() {
    let mut actual = Vec::new();
    const_for::const_for!(i in (-10..11).rev().step_by(5) => actual.push(i));
    assert_eq!(actual, vec![10, 5, 0, -5, -10]);
}
