use num::integer::Roots;
use num::traits::{CheckedAdd, CheckedDiv, CheckedMul, CheckedSub, One, Signed, ToPrimitive, Zero};
use std::sync::Mutex;

type R = num::rational::Rational64;
use crate::{ast::Literal, util::IndexSet};

use super::*;

lazy_static! {
    static ref RATIONAL_SORT_NAME: Symbol = "Rational".into();
    static ref RATS: Mutex<IndexSet<R>> = Default::default();
}

#[derive(Debug)]
pub struct RationalSort;

impl Sort for RationalSort {
    fn name(&self) -> Symbol {
        *RATIONAL_SORT_NAME
    }

    fn as_arc_any(self: Arc<Self>) -> Arc<dyn Any + Send + Sync + 'static> {
        self
    }

    #[rustfmt::skip]
    fn register_primitives(self: Arc<Self>, eg: &mut TypeInfo) {
        type Opt<T=()> = Option<T>;

        // TODO we can't have primitives take borrows just yet, since it
        // requires returning a reference to the locked sort
        add_primitives!(eg, "+" = |a: R, b: R| -> Opt<R> { a.checked_add(&b) });
        add_primitives!(eg, "-" = |a: R, b: R| -> Opt<R> { a.checked_sub(&b) });
        add_primitives!(eg, "*" = |a: R, b: R| -> Opt<R> { a.checked_mul(&b) });
        add_primitives!(eg, "/" = |a: R, b: R| -> Opt<R> { a.checked_div(&b) });

        add_primitives!(eg, "min" = |a: R, b: R| -> R { a.min(b) });
        add_primitives!(eg, "max" = |a: R, b: R| -> R { a.max(b) });
        add_primitives!(eg, "neg" = |a: R| -> R { -a });
        add_primitives!(eg, "abs" = |a: R| -> R { a.abs() });
        add_primitives!(eg, "floor" = |a: R| -> R { a.floor() });
        add_primitives!(eg, "ceil" = |a: R| -> R { a.ceil() });
        add_primitives!(eg, "round" = |a: R| -> R { a.round() });
        add_primitives!(eg, "rational" = |a: i64, b: i64| -> R { R::new(a, b) });
        add_primitives!(eg, "numer" = |a: R| -> i64 { *a.numer() });
        add_primitives!(eg, "denom" = |a: R| -> i64 { *a.denom() });

        add_primitives!(eg, "to-f64" = |a: R| -> f64 { a.to_f64().unwrap() });

        add_primitives!(eg, "pow" = |a: R, b: R| -> Option<R> {
            if a.is_zero() {
                if b.is_positive() {
                    Some(R::zero())
                } else {
                    None
                }
            } else if b.is_zero() {
                Some(R::one())
            } else if let Some(b) = b.to_i64() {
                if let Ok(b) = usize::try_from(b) {
                    num::traits::checked_pow(a, b)
                } else {
                    // TODO handle negative powers
                    None
                }
            } else {
                None
            }
        });
        add_primitives!(eg, "log" = |a: R| -> Option<R> {
            if a.is_one() {
                Some(R::zero())
            } else {
                todo!()
            }
        });
        add_primitives!(eg, "sqrt" = |a: R| -> Option<R> {
            if a.numer().is_positive() && a.denom().is_positive() {
                let s1 = a.numer().sqrt();
                let s2 = a.denom().sqrt();
                let is_perfect = &(s1 * s1) == a.numer() && &(s2 * s2) == a.denom();
                if is_perfect {
                    Some(R::new(s1, s2))
                } else {
                    None
                }
            } else {
                None
            }
        });
        add_primitives!(eg, "cbrt" = |a: R| -> Option<R> {
            if a.is_one() {
                Some(R::one())
            } else {
                todo!()
            }
        });

        add_primitives!(eg, "<" = |a: R, b: R| -> Opt { if a < b {Some(())} else {None} });
        add_primitives!(eg, ">" = |a: R, b: R| -> Opt { if a > b {Some(())} else {None} });
        add_primitives!(eg, "<=" = |a: R, b: R| -> Opt { if a <= b {Some(())} else {None} });
        add_primitives!(eg, ">=" = |a: R, b: R| -> Opt { if a >= b {Some(())} else {None} });
   }

    fn make_expr(&self, _egraph: &EGraph, value: Value) -> (Cost, Expr) {
        #[cfg(debug_assertions)]
        debug_assert_eq!(value.tag, self.name());

        let rat = R::load(self, &value);
        let numer = *rat.numer();
        let denom = *rat.denom();
        (
            1,
            Expr::call_no_span(
                "rational",
                vec![
                    GenericExpr::Lit(DUMMY_SPAN.clone(), Literal::Int(numer)),
                    GenericExpr::Lit(DUMMY_SPAN.clone(), Literal::Int(denom)),
                ],
            ),
        )
    }
}

impl FromSort for R {
    type Sort = RationalSort;
    fn load(_sort: &Self::Sort, value: &Value) -> Self {
        let i = value.bits as usize;
        *RATS.lock().unwrap().get_index(i).unwrap()
    }
}

impl IntoSort for R {
    type Sort = RationalSort;
    fn store(self, _sort: &Self::Sort) -> Option<Value> {
        let (i, _) = RATS.lock().unwrap().insert_full(self);
        Some(Value {
            #[cfg(debug_assertions)]
            tag: RationalSort.name(),
            bits: i as u64,
        })
    }
}
