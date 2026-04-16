from typing import List
from parser.ast import Expr, Identifier, Number, String, BinaryOp, UnaryOp, Comparison, FunctionCall

class ExprRewriter:
    """
    Base class for rewriting ActivityInfo expression ASTs.
    Traverses the AST and allows modification of nodes.
    If a node's children are unchanged, the node itself is returned (preserving identity).
    If children change, a new node is created.
    """

    def rewrite(self, expr: Expr) -> Expr:
        if isinstance(expr, Identifier):
            return self.visit_identifier(expr)
        if isinstance(expr, Number):
            return self.visit_number(expr)
        if isinstance(expr, String):
            return self.visit_string(expr)
        if isinstance(expr, BinaryOp):
            return self.visit_binary_op(expr)
        if isinstance(expr, UnaryOp):
            return self.visit_unary_op(expr)
        if isinstance(expr, Comparison):
            return self.visit_comparison(expr)
        if isinstance(expr, FunctionCall):
            return self.visit_function_call(expr)
        return expr

    def visit_identifier(self, expr: Identifier) -> Expr:
        return expr

    def visit_number(self, expr: Number) -> Expr:
        return expr

    def visit_string(self, expr: String) -> Expr:
        return expr

    def visit_binary_op(self, expr: BinaryOp) -> Expr:
        left = self.rewrite(expr.left)
        right = self.rewrite(expr.right)
        if left is expr.left and right is expr.right:
            return expr
        return BinaryOp(left, expr.op, right)

    def visit_unary_op(self, expr: UnaryOp) -> Expr:
        operand = self.rewrite(expr.operand)
        if operand is expr.operand:
            return expr
        return UnaryOp(expr.op, operand)

    def visit_comparison(self, expr: Comparison) -> Expr:
        left = self.rewrite(expr.left)
        right = self.rewrite(expr.right)
        if left is expr.left and right is expr.right:
            return expr
        return Comparison(left, expr.op, right)

    def visit_function_call(self, expr: FunctionCall) -> Expr:
        args = [self.rewrite(arg) for arg in expr.args]
        if all(a is b for a, b in zip(args, expr.args)):
            return expr
        return FunctionCall(expr.name, args)
