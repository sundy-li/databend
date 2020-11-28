// Copyright 2020 The FuseQuery Authors.
//
// Code is licensed under AGPL License, Version 3.0.

use log::error;

use sqlparser::ast;
use std::fmt;
use std::sync::Arc;

use crate::contexts::FuseQueryContext;
use crate::datavalues::DataValue;
use crate::error::{FuseQueryError, FuseQueryResult};
use crate::functions::{ConstantFunction, Function, ScalarFunctionFactory, VariableFunction};
use crate::planners::FormatterSettings;

#[derive(Clone)]
pub enum ExpressionPlan {
    /// Column field name in String.
    Field(String),

    /// Constant value in DataType.
    Constant(DataValue),

    BinaryExpression {
        op: String,
        left: Box<ExpressionPlan>,
        right: Box<ExpressionPlan>,
    },

    Function {
        op: String,
        args: Vec<ExpressionPlan>,
    },
}

impl ExpressionPlan {
    pub fn try_create(
        ctx: Arc<FuseQueryContext>,
        expr: &ast::Expr,
    ) -> FuseQueryResult<ExpressionPlan> {
        match expr {
            ast::Expr::Identifier(ref v) => Ok(ExpressionPlan::Field(v.clone().value)),
            ast::Expr::Value(ast::Value::Number(n)) => match n.parse::<i64>() {
                Ok(n) => Ok(ExpressionPlan::Constant(DataValue::Int64(Some(n)))),
                Err(_) => Ok(ExpressionPlan::Constant(DataValue::Float64(Some(
                    n.parse::<f64>()?,
                )))),
            },
            ast::Expr::Value(ast::Value::SingleQuotedString(s)) => {
                Ok(ExpressionPlan::Constant(DataValue::String(Some(s.clone()))))
            }
            ast::Expr::BinaryOp { left, op, right } => Ok(ExpressionPlan::BinaryExpression {
                op: format!("{}", op),
                left: Box::new(Self::try_create(ctx.clone(), left)?),
                right: Box::new(Self::try_create(ctx, right)?),
            }),
            ast::Expr::Nested(e) => Self::try_create(ctx, e),
            ast::Expr::Function(e) => {
                let mut args = Vec::with_capacity(e.args.len());
                for arg in &e.args {
                    args.push(ExpressionPlan::try_create(ctx.clone(), arg)?);
                }
                Ok(ExpressionPlan::Function {
                    op: e.name.to_string(),
                    args,
                })
            }
            _ => {
                error!("{:?}", expr);
                Err(FuseQueryError::Internal(format!(
                    "Unsupported ExpressionPlan: {}",
                    expr
                )))
            }
        }
    }

    pub fn to_function(&self) -> FuseQueryResult<Function> {
        match self {
            ExpressionPlan::Field(ref v) => VariableFunction::try_create(v.as_str()),
            ExpressionPlan::Constant(ref v) => ConstantFunction::try_create(v.clone()),
            ExpressionPlan::BinaryExpression { op, left, right } => {
                let l = left.to_function()?;
                let r = right.to_function()?;
                ScalarFunctionFactory::get(op, &[l, r])
            }
            ExpressionPlan::Function { op, args } => {
                let mut funcs = Vec::with_capacity(args.len());
                for arg in args {
                    funcs.push(arg.to_function()?);
                }
                ScalarFunctionFactory::get(op, &funcs)
            }
        }
    }

    pub fn name(&self) -> &'static str {
        "ExpressionPlan"
    }

    pub fn format(&self, f: &mut fmt::Formatter, _setting: &mut FormatterSettings) -> fmt::Result {
        write!(f, "")
    }
}

impl fmt::Debug for ExpressionPlan {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ExpressionPlan::Field(ref v) => write!(f, "{}", v),
            ExpressionPlan::Constant(ref v) => write!(f, "{:?}", v),
            ExpressionPlan::BinaryExpression { op, left, right } => {
                write!(f, "{:?} {} {:?}", left, op, right,)
            }
            ExpressionPlan::Function { op, args } => write!(f, "{} {:?}", op, args),
        }
    }
}

/// SQL.SelectItem to ExpressionStep.
pub fn item_to_expression_plan(
    ctx: Arc<FuseQueryContext>,
    item: &ast::SelectItem,
) -> FuseQueryResult<ExpressionPlan> {
    match item {
        ast::SelectItem::UnnamedExpr(expr) => ExpressionPlan::try_create(ctx, expr),
        _ => Err(FuseQueryError::Internal(format!(
            "Unsupported SelectItem {} in item_to_expression_step",
            item
        ))),
    }
}
