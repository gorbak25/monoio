use std::{io, marker::PhantomData};

#[cfg(all(target_os = "linux", feature = "iouring"))]
use crate::driver::IoUringDriver;
#[cfg(feature = "legacy")]
use crate::driver::LegacyDriver;
#[cfg(any(feature = "legacy", feature = "iouring"))]
use crate::utils::thread_id::gen_id;
use crate::{
    driver::{Driver, IntoInnerContext},
    time::{driver::TimeDriver, Clock},
    Runtime,
};

// ===== basic builder structure definition =====

/// Runtime builder
pub struct RuntimeBuilder<
    D,
    S: io_uring::squeue::EntryMarker = io_uring::squeue::Entry,
    C: io_uring::cqueue::EntryMarker = io_uring::cqueue::Entry,
> {
    // iouring entries
    entries: Option<u32>,

    #[cfg(all(target_os = "linux", feature = "iouring"))]
    urb: io_uring::Builder<S, C>,

    // blocking handle
    #[cfg(feature = "sync")]
    blocking_handle: crate::blocking::BlockingHandle,
    // driver mark
    _mark: PhantomData<D>,
}

scoped_thread_local!(pub(crate) static BUILD_THREAD_ID: usize);

impl<T, S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker> Default
    for RuntimeBuilder<T, S, C>
{
    /// Create a default runtime builder
    #[must_use]
    fn default() -> Self {
        RuntimeBuilder::<T, S, C>::new()
    }
}

impl<T, S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker>
    RuntimeBuilder<T, S, C>
{
    /// Create a default runtime builder
    #[must_use]
    pub fn new() -> Self {
        Self {
            entries: None,

            #[cfg(all(target_os = "linux", feature = "iouring"))]
            urb: io_uring::IoUring::<S, C>::builder(),

            #[cfg(feature = "sync")]
            blocking_handle: crate::blocking::BlockingStrategy::Panic.into(),
            _mark: PhantomData,
        }
    }
}

// ===== buildable trait and forward methods =====

/// Buildable trait.
pub trait Buildable<S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker>:
    Sized
{
    /// Build the runtime.
    fn build(this: RuntimeBuilder<Self, S, C>) -> io::Result<Runtime<Self>>;
}

#[allow(unused)]
macro_rules! direct_build {
    ($ty: ty) => {
        impl<S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker>
            RuntimeBuilder<$ty, S, C>
        where
            $ty: Buildable<S, C>,
        {
            /// Build the runtime.
            pub fn build(self) -> io::Result<Runtime<$ty>> {
                Buildable::build(self)
            }
        }
    };
}

#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry32>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(IoUringDriver<io_uring::squeue::Entry128, io_uring::cqueue::Entry>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(IoUringDriver<io_uring::squeue::Entry128, io_uring::cqueue::Entry32>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(TimeDriver<IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry>>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(TimeDriver<IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry32>>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(TimeDriver<IoUringDriver<io_uring::squeue::Entry128, io_uring::cqueue::Entry>>);
#[cfg(all(target_os = "linux", feature = "iouring"))]
direct_build!(TimeDriver<IoUringDriver<io_uring::squeue::Entry128, io_uring::cqueue::Entry32>>);
#[cfg(feature = "legacy")]
direct_build!(LegacyDriver);
#[cfg(feature = "legacy")]
direct_build!(TimeDriver<LegacyDriver>);

// ===== builder impl =====

#[cfg(feature = "legacy")]
impl<S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker> Buildable<S, C>
    for LegacyDriver
{
    fn build(this: RuntimeBuilder<Self, S, C>) -> io::Result<Runtime<LegacyDriver>> {
        let thread_id = gen_id();
        #[cfg(feature = "sync")]
        let blocking_handle = this.blocking_handle;

        BUILD_THREAD_ID.set(&thread_id, || {
            let driver = match this.entries {
                Some(entries) => LegacyDriver::new_with_entries(entries)?,
                None => LegacyDriver::new()?,
            };
            #[cfg(feature = "sync")]
            let context = crate::runtime::Context::new(blocking_handle);
            #[cfg(not(feature = "sync"))]
            let context = crate::runtime::Context::new();
            Ok(Runtime::new(context, driver))
        })
    }
}

#[cfg(all(target_os = "linux", feature = "iouring"))]
impl<S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker> Buildable<S, C>
    for IoUringDriver<S, C>
where
    IoUringDriver<S, C>: IntoInnerContext<S, C>,
{
    fn build(this: RuntimeBuilder<Self, S, C>) -> io::Result<Runtime<IoUringDriver<S, C>>> {
        let thread_id = gen_id();
        #[cfg(feature = "sync")]
        let blocking_handle = this.blocking_handle;

        BUILD_THREAD_ID.set(&thread_id, || {
            let driver = match this.entries {
                Some(entries) => IoUringDriver::new_with_entries(&this.urb, entries)?,
                None => IoUringDriver::new(&this.urb)?,
            };
            #[cfg(feature = "sync")]
            let context = crate::runtime::Context::new(blocking_handle);
            #[cfg(not(feature = "sync"))]
            let context = crate::runtime::Context::new();
            Ok(Runtime::new(context, driver))
        })
    }
}

impl<D, S: io_uring::squeue::EntryMarker, C: io_uring::cqueue::EntryMarker>
    RuntimeBuilder<D, S, C>
{
    const MIN_ENTRIES: u32 = 256;

    /// Set io_uring entries, min size is 256 and the default size is 1024.
    #[must_use]
    pub fn with_entries(mut self, entries: u32) -> Self {
        // If entries is less than 256, it will be 256.
        if entries < Self::MIN_ENTRIES {
            self.entries = Some(Self::MIN_ENTRIES);
            return self;
        }
        self.entries = Some(entries);
        self
    }

    /// Replaces the default [`io_uring::Builder`], which controls the settings for the
    /// inner `io_uring` API.
    ///
    /// Refer to the [`io_uring::Builder`] documentation for all the supported methods.

    #[cfg(all(target_os = "linux", feature = "iouring"))]
    #[must_use]
    pub fn uring_builder(mut self, urb: io_uring::Builder<S, C>) -> Self {
        self.urb = urb;
        self
    }
}

// ===== FusionDriver =====

/// Fake driver only for conditionally building.
#[cfg(any(all(target_os = "linux", feature = "iouring"), feature = "legacy"))]
pub struct FusionDriver;

#[cfg(any(all(target_os = "linux", feature = "iouring"), feature = "legacy"))]
impl RuntimeBuilder<FusionDriver, io_uring::squeue::Entry, io_uring::cqueue::Entry>
{
    /// Build the runtime.
    #[cfg(all(target_os = "linux", feature = "iouring", feature = "legacy"))]
    pub fn build(self) -> io::Result<crate::FusionRuntime<IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry>, LegacyDriver>> {
        if crate::utils::detect_uring() {
            let builder = RuntimeBuilder::<IoUringDriver<io_uring::squeue::Entry, io_uring::cqueue::Entry>, io_uring::squeue::Entry, io_uring::cqueue::Entry> {
                entries: self.entries,
                urb: self.urb,
                #[cfg(feature = "sync")]
                blocking_handle: self.blocking_handle,
                _mark: PhantomData,
            };
            info!("io_uring driver built");
            Ok(builder.build()?.into())
        } else {
            let builder = RuntimeBuilder::<LegacyDriver, io_uring::squeue::Entry, io_uring::cqueue::Entry> {
                entries: self.entries,
                urb: self.urb,
                #[cfg(feature = "sync")]
                blocking_handle: self.blocking_handle,
                _mark: PhantomData,
            };
            info!("legacy driver built");
            Ok(builder.build()?.into())
        }
    }

    /// Build the runtime.
    #[cfg(not(all(target_os = "linux", feature = "iouring")))]
    pub fn build(self) -> io::Result<crate::FusionRuntime<LegacyDriver>> {
        let builder = RuntimeBuilder::<LegacyDriver> {
            entries: self.entries,
            #[cfg(feature = "sync")]
            blocking_handle: self.blocking_handle,
            _mark: PhantomData,
        };
        Ok(builder.build()?.into())
    }

    /// Build the runtime.
    #[cfg(all(target_os = "linux", feature = "iouring", not(feature = "legacy")))]
    pub fn build(self) -> io::Result<crate::FusionRuntime<IoUringDriver>> {
        let builder = RuntimeBuilder::<IoUringDriver> {
            entries: self.entries,
            urb: self.urb,
            #[cfg(feature = "sync")]
            blocking_handle: self.blocking_handle,
            _mark: PhantomData,
        };
        Ok(builder.build()?.into())
    }
}

#[cfg(any(all(target_os = "linux", feature = "iouring"), feature = "legacy"))]
impl RuntimeBuilder<TimeDriver<FusionDriver>, io_uring::squeue::Entry, io_uring::cqueue::Entry> {
    /// Build the runtime.
    #[cfg(all(target_os = "linux", feature = "iouring", feature = "legacy"))]
    pub fn build(
        self,
    ) -> io::Result<crate::FusionRuntime<TimeDriver<IoUringDriver>, TimeDriver<LegacyDriver>>> {
        if crate::utils::detect_uring() {
            let builder = RuntimeBuilder::<TimeDriver<IoUringDriver>, io_uring::squeue::Entry, io_uring::cqueue::Entry> {
                entries: self.entries,
                urb: self.urb,
                #[cfg(feature = "sync")]
                blocking_handle: self.blocking_handle,
                _mark: PhantomData,
            };
            info!("io_uring driver with timer built");
            Ok(builder.build()?.into())
        } else {
            let builder = RuntimeBuilder::<TimeDriver<LegacyDriver>, io_uring::squeue::Entry, io_uring::cqueue::Entry> {
                entries: self.entries,
                urb: self.urb,
                #[cfg(feature = "sync")]
                blocking_handle: self.blocking_handle,
                _mark: PhantomData,
            };
            info!("legacy driver with timer built");
            Ok(builder.build()?.into())
        }
    }

    /// Build the runtime.
    #[cfg(not(all(target_os = "linux", feature = "iouring")))]
    pub fn build(self) -> io::Result<crate::FusionRuntime<TimeDriver<LegacyDriver>>> {
        let builder = RuntimeBuilder::<TimeDriver<LegacyDriver>> {
            entries: self.entries,
            #[cfg(feature = "sync")]
            blocking_handle: self.blocking_handle,
            _mark: PhantomData,
        };
        Ok(builder.build()?.into())
    }

    /// Build the runtime.
    #[cfg(all(target_os = "linux", feature = "iouring", not(feature = "legacy")))]
    pub fn build(self) -> io::Result<crate::FusionRuntime<TimeDriver<IoUringDriver>>> {
        let builder = RuntimeBuilder::<TimeDriver<IoUringDriver>> {
            entries: self.entries,
            urb: self.urb,
            #[cfg(feature = "sync")]
            blocking_handle: self.blocking_handle,
            _mark: PhantomData,
        };
        Ok(builder.build()?.into())
    }
}

// ===== enable_timer related =====
mod time_wrap {
    pub trait TimeWrapable {}
}

#[cfg(all(target_os = "linux", feature = "iouring"))]
impl time_wrap::TimeWrapable for IoUringDriver {}
#[cfg(feature = "legacy")]
impl time_wrap::TimeWrapable for LegacyDriver {}
#[cfg(any(all(target_os = "linux", feature = "iouring"), feature = "legacy"))]
impl time_wrap::TimeWrapable for FusionDriver {}

impl<D: Driver> Buildable<io_uring::squeue::Entry, io_uring::cqueue::Entry> for TimeDriver<D>
where
    D: Buildable<io_uring::squeue::Entry, io_uring::cqueue::Entry>,
{
    /// Build the runtime
    fn build(this: RuntimeBuilder<Self>) -> io::Result<Runtime<TimeDriver<D>>> {
        let Runtime {
            driver,
            mut context,
        } = Buildable::build(RuntimeBuilder::<D> {
            entries: this.entries,
            #[cfg(all(target_os = "linux", feature = "iouring"))]
            urb: this.urb,
            #[cfg(feature = "sync")]
            blocking_handle: this.blocking_handle,
            _mark: PhantomData,
        })?;

        let timer_driver = TimeDriver::new(driver, Clock::new());
        context.time_handle = Some(timer_driver.handle.clone());
        Ok(Runtime {
            driver: timer_driver,
            context,
        })
    }
}

impl<D: time_wrap::TimeWrapable> RuntimeBuilder<D> {
    /// Enable all(currently only timer)
    #[must_use]
    pub fn enable_all(self) -> RuntimeBuilder<TimeDriver<D>> {
        self.enable_timer()
    }

    /// Enable timer
    #[must_use]
    pub fn enable_timer(self) -> RuntimeBuilder<TimeDriver<D>> {
        let Self {
            entries,
            #[cfg(all(target_os = "linux", feature = "iouring"))]
            urb,
            #[cfg(feature = "sync")]
            blocking_handle,
            ..
        } = self;
        RuntimeBuilder {
            entries,
            #[cfg(all(target_os = "linux", feature = "iouring"))]
            urb,
            #[cfg(feature = "sync")]
            blocking_handle,
            _mark: PhantomData,
        }
    }
}

impl<D> RuntimeBuilder<D> {
    /// Attach thread pool, this will overwrite blocking strategy.
    /// All `spawn_blocking` will be executed on given thread pool.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn attach_thread_pool(
        mut self,
        tp: Box<dyn crate::blocking::ThreadPool + Send + 'static>,
    ) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Attached(tp);
        self
    }

    /// Set blocking strategy, this will overwrite thread pool setting.
    /// If `BlockingStrategy::Panic` is used, it will panic if `spawn_blocking` on this thread.
    /// If `BlockingStrategy::ExecuteLocal` is used, it will execute with current thread, and may
    /// cause tasks high latency.
    /// Attaching a thread pool is recommended if `spawn_blocking` will be used.
    #[cfg(feature = "sync")]
    #[must_use]
    pub fn with_blocking_strategy(mut self, strategy: crate::blocking::BlockingStrategy) -> Self {
        self.blocking_handle = crate::blocking::BlockingHandle::Empty(strategy);
        self
    }
}
