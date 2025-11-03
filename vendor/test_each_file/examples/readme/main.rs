fn main() {}

#[cfg(test)]
mod tests {
    mod file {
        use test_each_file::test_each_file;

        fn simple(input: &str) {
            assert!(input.split_whitespace().all(|n| n.parse::<usize>().is_ok()));
        }

        fn outlier([input]: [&str; 1]) {
            assert!(!input.is_empty());
        }

        fn complex([input, output]: [&str; 2]) {
            assert_eq!(
                input
                    .split_whitespace()
                    .map(|n| n.parse::<usize>().unwrap())
                    .sum::<usize>(),
                output.parse().unwrap()
            )
        }

        test_each_file! { in "./examples/readme/resources_simple/" as simple => simple}
        test_each_file! { for ["in", "out"] in "./examples/readme/resources_complex/" as complex => complex}
        test_each_file! { in "./examples/readme/resources_simple/" as closure => |c: &str| assert!(c.contains(" ")) }
        test_each_file! { for ["in", "out"] in "./examples/readme/resources_complex/" as example => |[a, b]: [&str; 2]| assert_ne!(a, b) }
        test_each_file! { for ["in"] in "./examples/readme/resources_with_outlier/" as outlier => outlier}
    }

    mod path {
        use std::fs::read_to_string;
        use std::path::Path;
        use test_each_file::test_each_path;

        fn simple(input: &Path) {
            assert!(read_to_string(input)
                .unwrap()
                .split_whitespace()
                .all(|n| n.parse::<usize>().is_ok()));
        }

        fn complex([input, output]: [&Path; 2]) {
            assert_eq!(
                read_to_string(input)
                    .unwrap()
                    .split_whitespace()
                    .map(|n| n.parse::<usize>().unwrap())
                    .sum::<usize>(),
                read_to_string(output).unwrap().parse().unwrap()
            )
        }

        test_each_path! { in "./examples/readme/resources_simple/" as simple => simple}
        test_each_path! { for ["in", "out"] in "./examples/readme/resources_complex/" as complex => complex}
    }

    mod duplicate_names {
        use test_each_file::test_each_file;

        fn empty(_input: &str) {}

        test_each_file! { in "./examples/readme/duplicate_names/" => empty}
    }

    mod attributes_async {
        use test_each_file::test_each_file;

        async fn run(input: &str) {
            assert!(input.split_whitespace().all(|n| n.parse::<usize>().is_ok()));
        }

        test_each_file! { #[tokio::test] async in "./examples/readme/resources_simple/" as simple => run }
    }
}
