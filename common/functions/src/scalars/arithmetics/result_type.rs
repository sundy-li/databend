// Copyright 2021 Datafuse Labs.
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

// This code is generated by common/codegen. DO NOT EDIT.
use common_datavalues::DFPrimitiveType;

pub trait ResultTypeOfArithmetic {
    type AddMul: DFPrimitiveType;
    type Minus: DFPrimitiveType;
    type IntDiv: DFPrimitiveType;
    type Modulo: DFPrimitiveType;
}

impl ResultTypeOfArithmetic for (u8, u8) {
    type AddMul = u16;
    type Minus = i16;
    type IntDiv = u8;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u8, u16) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u8, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u8, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u8, i8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u8, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u8, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u8, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u8, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u8, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u16, u8) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u16, u16) {
    type AddMul = u32;
    type Minus = i32;
    type IntDiv = u16;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u16, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u16, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u16, i8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u16, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u16, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u16, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u16, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u16, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u32, u8) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u32, u16) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u32, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u32, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u32, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u32, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u32, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u32, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u32, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u32, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u64, u8) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u64, u16) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u64, u32) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u64, u64) {
    type AddMul = u64;
    type Minus = i64;
    type IntDiv = u64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u64, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u8;
}

impl ResultTypeOfArithmetic for (u64, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u16;
}

impl ResultTypeOfArithmetic for (u64, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u32;
}

impl ResultTypeOfArithmetic for (u64, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = u64;
}

impl ResultTypeOfArithmetic for (u64, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (u64, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i8, u8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i8, u16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i8, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i8, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i8, i8) {
    type AddMul = i16;
    type Minus = i16;
    type IntDiv = i8;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i8, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i8, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i8, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i8, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i8, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i16, u8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i16, u16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i16, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i16, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i16, i8) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i16, i16) {
    type AddMul = i32;
    type Minus = i32;
    type IntDiv = i16;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i16, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i16, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i16, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i16, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i32, u8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i32, u16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i32, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i32, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i32, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i32, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i32, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i32;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i32, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i32, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i32, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i64, u8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i64, u16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i64, u32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i64, u64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i64, i8) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i16;
}

impl ResultTypeOfArithmetic for (i64, i16) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i32;
}

impl ResultTypeOfArithmetic for (i64, i32) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i64, i64) {
    type AddMul = i64;
    type Minus = i64;
    type IntDiv = i64;
    type Modulo = i64;
}

impl ResultTypeOfArithmetic for (i64, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (i64, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, u8) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, u16) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, u32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, u64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, i8) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, i16) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, i32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, i64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i32;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f32, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, u8) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, u16) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, u32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, u64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, i8) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, i16) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, i32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, i64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, f32) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}

impl ResultTypeOfArithmetic for (f64, f64) {
    type AddMul = f64;
    type Minus = f64;
    type IntDiv = i64;
    type Modulo = f64;
}
