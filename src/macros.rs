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
        // pretty_assertions::
        assert_eq!($exp1, $exp2);
        if $exp1 != $exp2 {
            return Err(failure::err_msg(format!("{} != {} {}:{}", stringify!($exp1), stringify!($exp2), file!(), line!())))
        }
    }};
}
