# sero-scheduler
A ScheduledExecutorService backed by a CachedThreadPool such that it automatically creates new threads as needed. Use as you would a ScheduledThreadPoolExecutor, except for two things:

1. Instead of using Executors to create, instantiate with new ScheduledExecutorServiceVariablePool()
2. ~~schedule(Callable&lt;V&gt;, long, TimeUnit) is currently not supported.~~ Actually, no, this is now supported. :)

