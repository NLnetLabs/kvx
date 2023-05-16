#[test]
fn test() {
    use trybuild;

    let t = trybuild::TestCases::new();
    t.compile_fail("tests/macro/fail-*.rs");
    t.pass("tests/macro/success.rs");
}
