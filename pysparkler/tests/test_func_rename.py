import libcst as cst


def test_func_rename():
    migration_pairs = [("toDegrees", "degrees"), ("toRadians", "radians")]
    make_func_rename(migration_pairs)


def make_func_rename(migration_pairs: list[tuple[str, str]]):
    print(f"Rewrite migration_pairs={migration_pairs}")

    # Parse the PySpark script written in version 2.4
    with open("tests/sample_inputs/old_funcs.py", encoding="utf-8") as f:
        original_code = f.read()
    original_tree = cst.parse_module(original_code)

    # Apply the re-writer to the AST
    rewriter = PySparkRewriter(migration_pairs)
    modified_tree = original_tree.visit(rewriter)

    # Verify the AST was modified
    modified_code = modified_tree.code
    assert modified_code is not None
    assert not modified_tree.deep_equals(original_tree)
    assert modified_code != original_code

    with open("tests/sample_inputs/new_funcs.py", encoding="utf-8") as f:
        expected_code = f.read()
        assert modified_code == expected_code


# Use the LibCST visitor pattern to traverse the AST and modify it
class PySparkRewriter(cst.CSTTransformer):
    def __init__(self, migration_pairs: list[tuple[str, str]]):
        super().__init__()
        self.migration_pairs = migration_pairs

    def leave_Call(
        self, original_node: cst.Call, updated_node: cst.Call
    ) -> cst.CSTNode:
        print(f"leave_Call: {original_node.func.value}")
        for old_name, new_name in self.migration_pairs:
            if original_node.func.value == old_name:
                updated_node = updated_node.with_changes(func=cst.Name(new_name))
        return updated_node
