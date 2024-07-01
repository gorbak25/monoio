use std::io;

#[cfg(all(target_os = "linux", feature = "iouring"))]
use io_uring::{opcode, types};

use super::{super::shared_fd::SharedFd, Op, OpAble};
use crate::driver::ready::Direction;

pub(crate) struct Cmd<T: Sized + Copy> {
    /// Holds a strong ref to the FD, preventing the file from being closed
    /// while the operation is in-flight.
    #[allow(unused)]
    fd: SharedFd,
    cmd_op: u32,
    pub(crate) cmd: T,
}

impl<T: Sized + Copy> Op<Cmd<T>> {
    pub(crate) fn issue_cmd(fd: &SharedFd, cmd_op: u32, cmd: T) -> io::Result<Op<Cmd<T>>> {
        Op::submit_with(Cmd {
            fd: fd.clone(),
            cmd_op,
            cmd,
        })
    }
}

impl<T: Sized + Copy> OpAble for Cmd<T> {
    #[cfg(all(target_os = "linux", feature = "iouring"))]
    fn uring_op(&mut self) -> io_uring::squeue::Entry {
        assert!(std::mem::size_of::<T>() <= 16, "Command does not fit into 64 byte submission queue entry. Have u considered expanding queue entries to 128 bytes?");
        #[repr(C)]
        union AsRawBytes<T: Copy> {cmd: T, buf: [u8; 16]}
        let t = AsRawBytes { cmd: self.cmd };
        opcode::UringCmd16::new(types::Fd(self.fd.raw_fd()), self.cmd_op)
            .cmd(unsafe { t.buf })
            .build()
    }

    #[cfg(all(target_os = "linux", feature = "iouring"))]
    fn uring_op_wide(&mut self) -> io_uring::squeue::Entry128 {
        assert!(std::mem::size_of::<T>() <= 80, "Command does not fit into 128 byte submission queue entry");
        #[repr(C)]
        union AsRawBytes<T: Copy> {cmd: T, buf: [u8; 80]}
        let t = AsRawBytes { cmd: self.cmd };
        opcode::UringCmd80::new(types::Fd(self.fd.raw_fd()), self.cmd_op)
            .cmd(unsafe { t.buf })
            .build()
    }

    #[cfg(any(feature = "legacy", feature = "poll-io"))]
    #[inline]
    fn legacy_interest(&self) -> Option<(Direction, usize)> {
        unimplemented!()
    }

    #[cfg(all(any(feature = "legacy", feature = "poll-io"), unix))]
    fn legacy_call(&mut self) -> io::Result<u32> {
        unimplemented!()
    }

    #[cfg(all(any(feature = "legacy", feature = "poll-io"), windows))]
    fn legacy_call(&mut self) -> io::Result<u32> {
        unimplemented!()
    }
}
