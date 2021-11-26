/*
* Copyright (C) 2019-2021 TON Labs. All Rights Reserved.
*
* Licensed under the SOFTWARE EVALUATION License (the "License"); you may not use
* this file except in compliance with the License.
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific TON DEV software governing permissions and
* limitations under the License.
*/

#[macro_export]
macro_rules! dump {
    ($data: expr) => {
        {
            let mut dump = String::new();
            for i in 0..$data.len() {
                dump.push_str(
                    &format!(
                        "{:02x}{}", 
                        $data[i], 
                        if (i + 1) % 16 == 0 { '\n' } else { ' ' }
                    )
                )
            }
            dump
        }
    };
    (debug, $target:expr, $msg:expr, $data:expr) => {
        if log::log_enabled!(log::Level::Debug) {
            log::debug!(target: $target, "{}:\n{}", $msg, dump!($data))
        }
    };
    (trace, $target:expr, $msg:expr, $data:expr) => {
        if log::log_enabled!(log::Level::Trace) {
            log::trace!(target: $target, "{}:\n{}", $msg, dump!($data))
        }
    }
}

#[macro_export]
macro_rules! CHECK {
    ($exp:expr) => {
        // TODO: remove for production
        if !($exp) {
            return Err(failure::err_msg(format!("{} {}:{}", stringify!($exp), file!(), line!())))
        }
    };
    ($exp:expr, inited) => {
        // TODO: remove for production
        if $exp == &Default::default() {
            return Err(failure::err_msg(format!("{} {}:{}", stringify!($exp), file!(), line!())))
        }
    };
    ($exp:expr, default) => {
        // TODO: remove for production
        if $exp != &Default::default() {
            return Err(failure::err_msg(format!("{} {}:{}", stringify!($exp), file!(), line!())))
        }
    };
    ($exp1:expr, $exp2:expr) => {{
        // TODO: remove for production
        if $exp1 != $exp2 {
            return Err(failure::err_msg(format!("{} != {} {}:{}", stringify!($exp1), stringify!($exp2), file!(), line!())))
        }
    }};
}
