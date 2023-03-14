// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashMap;
use std::fmt::Write;

use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use itertools::Itertools;

use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::decimal::DecimalScalar;
use crate::types::decimal::DecimalSize;
use crate::types::number::NumberDataType;
use crate::types::number::NumberScalar;
use crate::types::DataType;
use crate::types::DecimalDataType;
use crate::types::Number;
use crate::AutoCastRules;
use crate::ColumnIndex;
use crate::ConstantFolder;
use crate::FunctionContext;
use crate::Scalar;

pub fn check<Index: ColumnIndex>(
    ast: &RawExpr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    match ast {
        RawExpr::Literal { span, lit } => {
            let (scalar, data_type) = check_literal(lit);
            Ok(Expr::Constant {
                span: *span,
                scalar,
                data_type,
            })
        }
        RawExpr::ColumnRef {
            span,
            id,
            data_type,
            display_name,
        } => Ok(Expr::ColumnRef {
            span: *span,
            id: id.clone(),
            data_type: data_type.clone(),
            display_name: display_name.clone(),
        }),
        RawExpr::Cast {
            span,
            is_try,
            expr,
            dest_type,
        } => {
            let expr = check(expr, fn_registry)?;
            check_cast(*span, *is_try, expr, dest_type, fn_registry)
        }
        RawExpr::FunctionCall {
            span,
            name,
            args,
            params,
        } => {
            let mut args_expr: Vec<_> = args
                .iter()
                .map(|arg| check(arg, fn_registry))
                .try_collect()?;

            if name == "if" || name == "multi_if" {
                args_expr
                    .iter_mut()
                    .skip(1)
                    .step_by(2)
                    .for_each(|expr| *expr = expr.wrap_catch());
                let last = args_expr.last_mut().unwrap();
                *last = last.wrap_catch()
            }

            check_function(*span, name, params, &args_expr, fn_registry)
        }
    }
}

pub fn check_literal(literal: &Literal) -> (Scalar, DataType) {
    match literal {
        Literal::Null => (Scalar::Null, DataType::Null),
        Literal::UInt8(v) => (
            Scalar::Number(NumberScalar::UInt8(*v)),
            DataType::Number(NumberDataType::UInt8),
        ),
        Literal::UInt16(v) => (
            Scalar::Number(NumberScalar::UInt16(*v)),
            DataType::Number(NumberDataType::UInt16),
        ),
        Literal::UInt32(v) => (
            Scalar::Number(NumberScalar::UInt32(*v)),
            DataType::Number(NumberDataType::UInt32),
        ),
        Literal::UInt64(v) => (
            Scalar::Number(NumberScalar::UInt64(*v)),
            DataType::Number(NumberDataType::UInt64),
        ),
        Literal::Int8(v) => (
            Scalar::Number(NumberScalar::Int8(*v)),
            DataType::Number(NumberDataType::Int8),
        ),
        Literal::Int16(v) => (
            Scalar::Number(NumberScalar::Int16(*v)),
            DataType::Number(NumberDataType::Int16),
        ),
        Literal::Int32(v) => (
            Scalar::Number(NumberScalar::Int32(*v)),
            DataType::Number(NumberDataType::Int32),
        ),
        Literal::Int64(v) => (
            Scalar::Number(NumberScalar::Int64(*v)),
            DataType::Number(NumberDataType::Int64),
        ),
        Literal::Decimal128 {
            value,
            precision,
            scale,
        } => {
            let size = DecimalSize {
                precision: *precision,
                scale: *scale,
            };
            (
                Scalar::Decimal(DecimalScalar::Decimal128(*value, size)),
                DataType::Decimal(DecimalDataType::Decimal128(size)),
            )
        }
        Literal::Decimal256 {
            value,
            precision,
            scale,
        } => {
            let size = DecimalSize {
                precision: *precision,
                scale: *scale,
            };
            (
                Scalar::Decimal(DecimalScalar::Decimal256(*value, size)),
                DataType::Decimal(DecimalDataType::Decimal256(size)),
            )
        }
        Literal::Float32(v) => (
            Scalar::Number(NumberScalar::Float32(*v)),
            DataType::Number(NumberDataType::Float32),
        ),
        Literal::Float64(v) => (
            Scalar::Number(NumberScalar::Float64(*v)),
            DataType::Number(NumberDataType::Float64),
        ),
        Literal::Boolean(v) => (Scalar::Boolean(*v), DataType::Boolean),
        Literal::String(v) => (Scalar::String(v.clone()), DataType::String),
    }
}

pub fn check_cast<Index: ColumnIndex>(
    span: Span,
    is_try: bool,
    expr: Expr<Index>,
    dest_type: &DataType,
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    let wrapped_dest_type = if is_try {
        wrap_nullable_for_try_cast(span, dest_type)?
    } else {
        dest_type.clone()
    };

    if expr.data_type() == &wrapped_dest_type {
        Ok(expr)
    } else if expr.data_type().wrap_nullable() == wrapped_dest_type {
        Ok(Expr::Cast {
            span,
            is_try,
            expr: Box::new(expr),
            dest_type: wrapped_dest_type,
        })
    } else {
        // fast path to eval function for cast
        if let Some(cast_fn) = get_simple_cast_function(is_try, dest_type) {
            let params = if let DataType::Decimal(ty) = dest_type {
                vec![ty.precision() as usize, ty.scale() as usize]
            } else {
                vec![]
            };

            if let Ok(cast_expr) =
                check_function(span, &cast_fn, &params, &[expr.clone()], fn_registry)
            {
                if cast_expr.data_type() == &wrapped_dest_type {
                    return Ok(cast_expr);
                }
            }
        }

        Ok(Expr::Cast {
            span,
            is_try,
            expr: Box::new(expr),
            dest_type: wrapped_dest_type,
        })
    }
}

fn wrap_nullable_for_try_cast(span: Span, ty: &DataType) -> Result<DataType> {
    match ty {
        DataType::Null => Err(ErrorCode::from_string_no_backtrace(
            "TRY_CAST() to NULL is not supported".to_string(),
        )
        .set_span(span)),
        DataType::Nullable(ty) => wrap_nullable_for_try_cast(span, ty),
        DataType::Array(inner_ty) => Ok(DataType::Nullable(Box::new(DataType::Array(Box::new(
            wrap_nullable_for_try_cast(span, inner_ty)?,
        ))))),
        DataType::Tuple(fields_ty) => Ok(DataType::Nullable(Box::new(DataType::Tuple(
            fields_ty
                .iter()
                .map(|ty| wrap_nullable_for_try_cast(span, ty))
                .collect::<Result<Vec<_>>>()?,
        )))),
        _ => Ok(DataType::Nullable(Box::new(ty.clone()))),
    }
}

pub fn check_number<Index: ColumnIndex, T: Number>(
    span: Span,
    func_ctx: FunctionContext,
    expr: &Expr<Index>,
    fn_registry: &FunctionRegistry,
) -> Result<T> {
    let (expr, _) = ConstantFolder::fold(expr, func_ctx, fn_registry);
    match expr {
        Expr::Constant {
            scalar: Scalar::Number(num),
            ..
        } => T::try_downcast_scalar(&num).ok_or_else(|| {
            ErrorCode::InvalidArgument(format!(
                "Expect {}, but got {}",
                T::data_type(),
                expr.data_type()
            ))
            .set_span(span)
        }),
        _ => Err(ErrorCode::InvalidArgument(format!(
            "Expect {}, but got {}",
            T::data_type(),
            expr.data_type()
        ))
        .set_span(span)),
    }
}

pub fn check_function<Index: ColumnIndex>(
    span: Span,
    name: &str,
    params: &[usize],
    args: &[Expr<Index>],
    fn_registry: &FunctionRegistry,
) -> Result<Expr<Index>> {
    if let Some(original_fn_name) = fn_registry.aliases.get(name) {
        return check_function(span, original_fn_name, params, args, fn_registry);
    }

    let candidates = fn_registry.search_candidates(name, params, args);

    if candidates.is_empty() && !fn_registry.contains(name) {
        return Err(
            ErrorCode::UnknownFunction(format!("function `{name}` does not exist")).set_span(span),
        );
    }

    let auto_cast_rules = fn_registry.get_auto_cast_rules(name);

    let mut fail_resaons = Vec::with_capacity(candidates.len());
    for (id, func) in &candidates {
        match try_check_function(span, args, &func.signature, auto_cast_rules, fn_registry) {
            Ok((checked_args, return_type, generics)) => {
                return Ok(Expr::FunctionCall {
                    span,
                    id: id.clone(),
                    function: func.clone(),
                    generics,
                    args: checked_args,
                    return_type,
                });
            }
            Err(err) => fail_resaons.push(err),
        }
    }

    let mut msg = if params.is_empty() {
        format!(
            "no overload satisfies `{name}({})`",
            args.iter()
                .map(|arg| arg.data_type().to_string())
                .join(", ")
        )
    } else {
        format!(
            "no overload satisfies `{name}({})({})`",
            params.iter().join(", "),
            args.iter()
                .map(|arg| arg.data_type().to_string())
                .join(", ")
        )
    };
    if !candidates.is_empty() {
        let candidates_sig: Vec<_> = candidates
            .iter()
            .map(|(_, func)| func.signature.to_string())
            .collect();

        let max_len = candidates_sig.iter().map(|s| s.len()).max().unwrap_or(0);

        let candidates_fail_reason = candidates_sig
            .into_iter()
            .zip(fail_resaons)
            .map(|(sig, err)| format!("  {sig:<max_len$}  : {}", err.message()))
            .join("\n");

        write!(
            &mut msg,
            "\n\nhas tried possible overloads:\n{}",
            candidates_fail_reason
        )
        .unwrap();
    };

    Err(ErrorCode::SemanticError(msg).set_span(span))
}

#[derive(Debug)]
pub struct Substitution(pub HashMap<usize, DataType>);

impl Substitution {
    pub fn empty() -> Self {
        Substitution(HashMap::new())
    }

    pub fn equation(idx: usize, ty: DataType) -> Self {
        let mut subst = Self::empty();
        subst.0.insert(idx, ty);
        subst
    }

    pub fn merge(mut self, other: Self, auto_cast_rules: AutoCastRules) -> Result<Self> {
        for (idx, ty2) in other.0 {
            if let Some(ty1) = self.0.remove(&idx) {
                let common_ty = common_super_type(ty2.clone(), ty1.clone(), auto_cast_rules)
                    .ok_or_else(|| {
                        ErrorCode::from_string_no_backtrace(format!(
                            "unable to find a common super type for `{ty1}` and `{ty2}`"
                        ))
                    })?;
                self.0.insert(idx, common_ty);
            } else {
                self.0.insert(idx, ty2);
            }
        }

        Ok(self)
    }

    pub fn apply(&self, ty: &DataType) -> Result<DataType> {
        match ty {
            DataType::Generic(idx) => self.0.get(idx).cloned().ok_or_else(|| {
                ErrorCode::from_string_no_backtrace(format!("unbound generic type `T{idx}`"))
            }),
            DataType::Nullable(box ty) => Ok(DataType::Nullable(Box::new(self.apply(ty)?))),
            DataType::Array(box ty) => Ok(DataType::Array(Box::new(self.apply(ty)?))),
            DataType::Map(box ty) => {
                let inner_ty = self.apply(ty)?;
                Ok(DataType::Map(Box::new(inner_ty)))
            }
            DataType::Tuple(fields_ty) => {
                let fields_ty = fields_ty
                    .iter()
                    .map(|field_ty| self.apply(field_ty))
                    .collect::<Result<_>>()?;
                Ok(DataType::Tuple(fields_ty))
            }
            ty => Ok(ty.clone()),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function<Index: ColumnIndex>(
    span: Span,
    args: &[Expr<Index>],
    sig: &FunctionSignature,
    auto_cast_rules: AutoCastRules,
    fn_registry: &FunctionRegistry,
) -> Result<(Vec<Expr<Index>>, DataType, Vec<DataType>)> {
    let subst = try_unify_signature(
        args.iter().map(Expr::data_type),
        sig.args_type.iter(),
        auto_cast_rules,
    )?;

    let checked_args = args
        .iter()
        .zip(&sig.args_type)
        .map(|(arg, sig_type)| {
            let sig_type = subst.apply(sig_type)?;
            let is_try = fn_registry.is_auto_try_cast_rule(arg.data_type(), &sig_type);
            check_cast(span, is_try, arg.clone(), &sig_type, fn_registry)
        })
        .collect::<Result<Vec<_>>>()?;

    let return_type = subst.apply(&sig.return_type)?;
    assert!(!return_type.has_nested_nullable());

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| match subst.0.get(&idx) {
                    Some(ty) => Ok(ty.clone()),
                    None => Err(ErrorCode::from_string_no_backtrace(format!(
                        "unable to resolve generic T{idx}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    Ok((checked_args, return_type, generics))
}

pub fn try_unify_signature(
    src_tys: impl IntoIterator<Item = &DataType> + ExactSizeIterator,
    dest_tys: impl IntoIterator<Item = &DataType> + ExactSizeIterator,
    auto_cast_rules: AutoCastRules,
) -> Result<Substitution> {
    assert_eq!(src_tys.len(), dest_tys.len());

    let substs = src_tys
        .into_iter()
        .zip(dest_tys)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty, auto_cast_rules))
        .collect::<Result<Vec<_>>>()?;

    Ok(substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2, auto_cast_rules))?
        .unwrap_or_else(Substitution::empty))
}

pub fn unify(
    src_ty: &DataType,
    dest_ty: &DataType,
    auto_cast_rules: AutoCastRules,
) -> Result<Substitution> {
    match (src_ty, dest_ty) {
        (DataType::Generic(_), _) => Err(ErrorCode::from_string_no_backtrace(
            "source type {src_ty} must not contain generic type".to_string(),
        )),
        (ty, DataType::Generic(_)) if ty.has_generic() => Err(ErrorCode::from_string_no_backtrace(
            "source type {src_ty} must not contain generic type".to_string(),
        )),
        (ty, DataType::Generic(idx)) => Ok(Substitution::equation(*idx, ty.clone())),
        (src_ty, dest_ty) if can_auto_cast_to(src_ty, dest_ty, auto_cast_rules) => {
            Ok(Substitution::empty())
        }
        (DataType::Null, DataType::Nullable(_)) => Ok(Substitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Ok(Substitution::empty()),
        (DataType::EmptyMap, DataType::Map(_)) => Ok(Substitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            unify(src_ty, dest_ty, auto_cast_rules)
        }
        (src_ty, DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty, auto_cast_rules),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => {
            unify(src_ty, dest_ty, auto_cast_rules)
        }
        (DataType::Map(box src_ty), DataType::Map(box dest_ty)) => match (src_ty, dest_ty) {
            (DataType::Tuple(_), DataType::Tuple(_)) => unify(src_ty, dest_ty, auto_cast_rules),
            (_, _) => unreachable!(),
        },
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty, auto_cast_rules))
                .collect::<Result<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2, auto_cast_rules))?
                .unwrap_or_else(Substitution::empty);
            Ok(subst)
        }
        _ => Err(ErrorCode::from_string_no_backtrace(format!(
            "unable to unify `{}` with `{}`",
            src_ty, dest_ty
        ))),
    }
}

pub fn can_auto_cast_to(
    src_ty: &DataType,
    dest_ty: &DataType,
    auto_cast_rules: AutoCastRules,
) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,
        (src_ty, dest_ty)
            if auto_cast_rules
                .iter()
                .any(|(src, dest)| src == src_ty && dest == dest_ty) =>
        {
            true
        }
        (DataType::Null, DataType::Nullable(_)) => true,
        (DataType::EmptyArray, DataType::Array(_)) => true,
        (DataType::EmptyMap, DataType::Map(_)) => true,
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty, auto_cast_rules)
        }
        (src_ty, DataType::Nullable(dest_ty)) => can_auto_cast_to(src_ty, dest_ty, auto_cast_rules),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => {
            can_auto_cast_to(src_ty, dest_ty, auto_cast_rules)
        }
        (DataType::Map(box src_ty), DataType::Map(box dest_ty)) => match (src_ty, dest_ty) {
            (DataType::Tuple(_), DataType::Tuple(_)) => {
                can_auto_cast_to(src_ty, dest_ty, auto_cast_rules)
            }
            (_, _) => unreachable!(),
        },
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            src_tys
                .iter()
                .zip(dest_tys)
                .all(|(src_ty, dest_ty)| can_auto_cast_to(src_ty, dest_ty, auto_cast_rules))
        }
        (DataType::String, DataType::Decimal(_)) => true,
        (DataType::Decimal(x), DataType::Decimal(y)) => x.precision() <= y.precision(),
        (DataType::Number(n), DataType::Decimal(_)) if !n.is_float() => true,
        (DataType::Decimal(_), DataType::Number(n)) if n.is_float() => true,
        _ => false,
    }
}

pub fn common_super_type(
    ty1: DataType,
    ty2: DataType,
    auto_cast_rules: AutoCastRules,
) -> Option<DataType> {
    match (ty1, ty2) {
        (ty1, ty2) if can_auto_cast_to(&ty1, &ty2, auto_cast_rules) => Some(ty2),
        (ty1, ty2) if can_auto_cast_to(&ty2, &ty1, auto_cast_rules) => Some(ty1),
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),
        (DataType::Nullable(box ty1), DataType::Nullable(box ty2))
        | (DataType::Nullable(box ty1), ty2)
        | (ty1, DataType::Nullable(box ty2)) => Some(DataType::Nullable(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(box ty1), DataType::Array(box ty2)) => Some(DataType::Array(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::EmptyMap, ty @ DataType::Map(_))
        | (ty @ DataType::Map(_), DataType::EmptyMap) => Some(ty),
        (DataType::Map(box ty1), DataType::Map(box ty2)) => Some(DataType::Map(Box::new(
            common_super_type(ty1, ty2, auto_cast_rules)?,
        ))),
        (DataType::Tuple(tys1), DataType::Tuple(tys2)) if tys1.len() == tys2.len() => {
            let tys = tys1
                .into_iter()
                .zip(tys2)
                .map(|(ty1, ty2)| common_super_type(ty1, ty2, auto_cast_rules))
                .collect::<Option<Vec<_>>>()?;
            Some(DataType::Tuple(tys))
        }
        (DataType::String, decimal_ty @ DataType::Decimal(_))
        | (decimal_ty @ DataType::Decimal(_), DataType::String) => Some(decimal_ty),
        (DataType::Decimal(a), DataType::Decimal(b)) => {
            let ty = DecimalDataType::binary_result_type(&a, &b, false, false, true).ok();
            ty.map(DataType::Decimal)
        }
        (DataType::Number(num_ty), DataType::Decimal(decimal_ty))
        | (DataType::Decimal(decimal_ty), DataType::Number(num_ty))
            if !num_ty.is_float() =>
        {
            let max_precision = decimal_ty.max_precision();
            let scale = decimal_ty.scale();
            DecimalDataType::from_size(DecimalSize {
                precision: max_precision,
                scale,
            })
            .ok()
            .map(DataType::Decimal)
        }
        (DataType::Number(num_ty), DataType::Decimal(_))
        | (DataType::Decimal(_), DataType::Number(num_ty))
            if num_ty.is_float() =>
        {
            Some(DataType::Number(num_ty))
        }
        (ty1, ty2) => {
            let ty1_can_cast_to = auto_cast_rules
                .iter()
                .filter(|(src, _)| *src == ty1)
                .map(|(_, dest)| dest)
                .collect::<Vec<_>>();
            let ty2_can_cast_to = auto_cast_rules
                .iter()
                .filter(|(src, _)| *src == ty2)
                .map(|(_, dest)| dest)
                .collect::<Vec<_>>();
            ty1_can_cast_to
                .into_iter()
                .find(|ty| ty2_can_cast_to.contains(ty))
                .cloned()
        }
    }
}

pub fn get_simple_cast_function(is_try: bool, dest_type: &DataType) -> Option<String> {
    let function_name = if dest_type.is_decimal() {
        "to_decimal".to_owned()
    } else {
        format!("to_{}", dest_type.to_string().to_lowercase())
    };

    if is_simple_cast_function(&function_name) {
        let prefix = if is_try { "try_" } else { "" };
        Some(format!("{prefix}{function_name}"))
    } else {
        None
    }
}

pub const ALL_SIMPLE_CAST_FUNCTIONS: &[&str] = &[
    "to_string",
    "to_uint8",
    "to_uint16",
    "to_uint32",
    "to_uint64",
    "to_int8",
    "to_int16",
    "to_int32",
    "to_int64",
    "to_float32",
    "to_float64",
    "to_timestamp",
    "to_date",
    "to_variant",
    "to_boolean",
    "to_decimal",
];

pub fn is_simple_cast_function(name: &str) -> bool {
    ALL_SIMPLE_CAST_FUNCTIONS.contains(&name)
}
