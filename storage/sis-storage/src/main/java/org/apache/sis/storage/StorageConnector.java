package org.apache.sis.storage;

import java.io.File;
import java.io.IOException;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.imageio.stream.ImageInputStream;
import javax.sql.DataSource;
import org.apache.sis.internal.storage.io.ChannelDataInput;
import org.apache.sis.setup.OptionKey;
import org.apache.sis.util.UnconvertibleObjectException;
import org.apache.sis.util.collection.BackingStoreException;

/**
 * Extension of a storage connector providing strong encapsulation of "views". This allows to:
 * <ul>
 *     <li>Adopt a <em>fail-fast</em> behavior in case storage view is corrupted by a user</li>
 *     <li>
 *         Provide easier usage:
 *         <ul>
 *             <li>Initial mark/final rewind is performed internally, user do not need to care about it.</li>
 *             <li>Provide strongly typed operators, to guide user on how to use this object.</li>
 *         </ul>
 *     </li>
 * </ul>
 * The purpose of this class is to be merged in StorageConnector once its principle has been validated.
 *
 * <em>Guarantees</em>:
 * <ul>
 *     <li>This object is <em>not</em> concurrent, and ensure a <em>fail-fast</em> behavior in such cases.</li>
 *     <li>
 *         useAs* methods will enforce following behavior:
 *         <ul>
 *             <li>If possible, rewind properly consumed storage view to its initial state</li>
 *             <li>If above statement is not possible, an error will be immediately propagated, and the connector will be marked as closed.</li>
 *         </ul>
 *     </li>
 *     <li>
 *         Thread-safety: This object is <em>not</em> thread-safe.
 *         However, controls are done upon usage to ensure that at most one operation is going on it at any time.
 *         Note that there is two consequences for this:
 *         <ol>
 *             <li>This object is <em>not reentrant, even in single-thread context</em>. That means that you cannot nest calls to useAs.</li>
 *             <li>Any attempt to use this storage concurrently will result in a {@link ConcurrentReadException}.</li>
 *         </ol>
 *     </li>
 * </ul>
 *
 * Typical usage:
 * <ol>
 *     <li>Check storage compatibility through `useAs*` methods</li>
 *     <li>If the storage is compatible, commit our choice by locking a storage view, closing the connector in the process.</li>
 * </ol>
 *
 * Example:
 * <pre>
 * final Path file = Paths.get("path/to/file");
 * try (var c = new StrictStorageConnector(new StorageConnector(file)) {
 *
 *   // Use connector automatically reset buffering to check support
 *   Boolean isSupported = c.useAsBuffer((buffer) -%gt; buffer.get() == SEARCHED_KEY);
 *
 *   // Once support is validated, acquire real storage connection. At this point,
 *   // storage life cycle becomes the responsability of the caller, allowing it
 *   // to survive beyond the connector scope.
 *   if (supported) {
 *     try ( InputStream stream = c.commit( InputStream.class ) ) {
 *         // read all needed data from acquired stream
 *     }
 *   }
 * }
 * </pre>
 */
public class StorageConnector implements AutoCloseable {

    private final UnsafeConnector storage;

    private WeakReference<Object> committedStorage;

    /**
     * Indicates usage status of the connector:
     * <ul>
     *     <li><em>-1</em>: The connector is closed. Resources it has allocated have been disposed, and it cannot be used anymore.</li>
     *     <li><em>0</em>: Connector is usable, no processing by any thread for the moment</li>
     *     <li><em>1</em>: The connector is being used by a thread. In this case, any other attempt to use it is bound to fail.</li>
     * </ul>
     */
    private final AtomicInteger stateFlag = new AtomicInteger();

    public StorageConnector(Object storage) {
        this(storage instanceof UnsafeConnector ? (UnsafeConnector) storage : new UnsafeConnector(storage));
    }

    public StorageConnector(UnsafeConnector storage) {
        this.storage = storage;
    }

    public void closeAllExcept(Object view) throws DataStoreException {
        // Closing multiple times is OK. However, if the view is not null, we will let control raise an error.
        if (stateFlag.get() < 0 && view == null) return;
        try {
            doUnderControl(() -> {
                stateFlag.set(-1);
                storage.closeAllExcept(view);
                committedStorage = (view == null ? null : new WeakReference<>(view));
                return null;
            });
        } catch (IOException e) {
            throw new DataStoreException(e);
        }
    }

    /**
     * Provides an in-memory byte buffer containing first bytes of the source storage.
     * To know how many bytes are available, refer to the buffer {@link ByteBuffer#remaining() remaining byte count}.
     * User <em>do not</em> need to rewind buffer after use. It is the storage connector responsability.
     *
     * @param operator User operation to perform against provided buffer.
     * @param <T> Type of result produced by user operator.
     * @return The value computed by input operator.
     * @throws UnsupportedStorageException If queried storage type cannot be accessed in current context.
     * @throws DataStoreException If an error occurs while fetching queried storage.
     */
    public <T> T useAsBuffer(StorageOperatingFunction<? super ByteBuffer, T> operator) throws DataStoreException {
        return tryUseAsBuffer(operator).getOrThrow();
    }

    public <T> Result<T> tryUseAsBuffer(StorageOperatingFunction<? super ByteBuffer, ? extends T> operator) throws DataStoreException {
        return use(ByteBuffer.class, new ByteBufferControl(), operator);
    }

    /**
     * Specialization of {@link #useAs(Class, StorageOperatingFunction)} for {@link ImageInputStream ImageIO API}.
     */
    public <T> T useAsImageInputStream(StorageOperatingFunction<? super ImageInputStream, T> operator) throws DataStoreException {
        return tryUseAsImageInputStream(operator).getOrThrow();
    }

    public <T> Result<T> tryUseAsImageInputStream(StorageOperatingFunction<? super ImageInputStream, T> operator) throws DataStoreException {
        return use(ImageInputStream.class, new StreamControl<>(ImageInputStream.class, IISAdapter::new), operator);
    }

    /**
     * Temporarily expose storage through queried interface/class to be used by a user defined operator.
     * Notes:
     * <ul>
     *     <li>
     *         This method handles mark before / rewind after usage. User responsability is to not leave additional
     *         marks unrewinded. Therefore, if you just need to sequentially read input, you don't have to mark/rewind
     *         the storage.
     *     </li>
     *     <li>
     *         Provided storage is checked after use, to ensure it has not been corrupted by input operator. If control
     *         fails, a {@link StorageControlException} is raised.
     *     </li>
     * </ul>
     *
     * @param storageType Storage access interface to provide to the operator.
     * @param operator The operator that will access storage to compute a result.
     * @param <S> Storage class
     * @param <T> Type of result computed by user operator.
     * @return The value computed by user operator.
     * @throws UnsupportedStorageException If queried storage type cannot be accessed in current context.
     * @throws StorageControlException If connector has detected storage corruption after operator usage.
     * @throws DataStoreException If an error occurs while fetching queried storage.
     */
    public <S, T> T useAs(Class<S> storageType, StorageOperatingFunction<? super S, ? extends T> operator) throws DataStoreException {
        final Result<T> r = tryUseAs(storageType, operator);
        if (r instanceof Failure) throw new UnsupportedStorageException("Queried storage type is not supported yet: "+storageType);
        else return r.getOrThrow();
    }

    @SuppressWarnings("unchecked")
    public <I, O> Result<O> tryUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) throws DataStoreException {
        if (ByteBuffer.class.equals(storageType)) {
            return use(ByteBuffer.class, new ByteBufferControl(), (StorageOperatingFunction<? super ByteBuffer, ? extends O>) operator);
        } else if (ImageInputStream.class.isAssignableFrom(storageType)) {
            return use(storageType, new StreamControl<>(storageType, stream -> new IISAdapter((ImageInputStream) stream)), operator);
        } else if (ChannelDataInput.class.isAssignableFrom(storageType)) {
            return use(storageType, new StreamControl<>(storageType, stream -> new CDIAdapter((ChannelDataInput) stream)), operator);
        } else if (SeekableByteChannel.class.isAssignableFrom(storageType)) {
            return use(storageType, new StreamControl<>(storageType, stream -> new SBCAdapter((SeekableByteChannel) stream)), operator);
        } else if (URI.class.isAssignableFrom(storageType)) {
            final URI input = getURI().orElseThrow(() -> new UnsupportedStorageException("Cannot acquire an URI"));
            return new Success<>(((StorageOperatingFunction<? super URI, O>) operator).applyCatchingIO(input));
        } else if (Path.class.isAssignableFrom(storageType)) {
            final Path input = getPath().orElseThrow(() -> new UnsupportedStorageException("Cannot acquire a path"));
            return new Success<>(((StorageOperatingFunction<? super Path, O>) operator).applyCatchingIO(input));
        }
        else if (File.class.isAssignableFrom(storageType)) {
            final Path input = getPath().orElseThrow(() -> new UnsupportedStorageException("Cannot acquire a path"));
            final File inputFile;
            try {
                inputFile = input.toFile();
            } catch (UnsupportedOperationException e) {
                throw new UnsupportedStorageException("Storage is not a local path (not a file on default file-system).", e);
            }
            return new Success<>(((StorageOperatingFunction<? super File, O>) operator).applyCatchingIO(inputFile));
        }

        throw new UnsupportedStorageException("Queried storage type is not supported yet: "+storageType);
    }


    private <I, O> Result<O> use(Class<I> storageType, ControlStrategy<I> control, StorageOperatingFunction<? super I, ? extends O> action) throws DataStoreException {
        try {
            return doUnderControl(() -> {
                final I rawInput = getOrFail(storageType, null, null);
                final ControlOperator<I> op = control.init(rawInput);
                try {
                    return new Success<>(action.apply(op.getStorage()));
                } catch (DataStoreException | IOException | RuntimeException e) {
                    // Priorize errors that denote a corruption of the storage over any other exception thrown by the operator.
                    try {
                        op.postControl();
                        throw e;
                    } catch (StorageControlException ctrlError) {
                        stateFlag.set(-1);
                        ctrlError.addSuppressed(e);
                        throw ctrlError;
                    }
                } finally {
                    try {
                        op.postControl();
                    } catch (StorageControlException ctrlError) {
                        stateFlag.set(-1);
                        throw ctrlError;
                    }
                }
            });
        } catch (IOException e) {
            throw new DataStoreException("Error while accessing storage", e);
        }
    }

    /**
     * Ensure only one storage operation is running at any time against this storage connector. It allows fail-fast
     * behavior if this connector is used in concurrent context.
     *
     * @param operator The operation to perform once we checked no other operation is running.
     *
     * @param <V> Type of result value produced by given operator.
     * @return The result produced by given operator.
     * @throws IOException If anything wrong happens while input operator consumes storage, or we can mark/rewind storage.
     * @throws DataStoreException Same reasons as for IOException + can happen if queried storage is of unsupported type.
     * @throws IllegalStateException If this connector is already closed.
     */
    protected <V> V doUnderControl(StorageCallable<? extends V> operator) throws IOException, DataStoreException {
        if (stateFlag.compareAndSet(0, 1)) {
            try {
                return operator.call();
            } finally {
                /* It is possible that the operation has closed this storage connector. In such case, we must not
                 * reset value. In case something really bad happened, and the value has grown over 1, the storage
                 * connector becomes invalid, and any further attempt to use it will fail (see else clause below).
                 */
                stateFlag.compareAndSet(1, 0);
            }
        } else {
            final int state = stateFlag.get();
            if (state < 0) throw new IllegalStateException("Storage is closed. No more operation can be initiated.");
            else if (state > 0) throw new ConcurrentReadException("Another context is already using this storage connector");
            else throw new IllegalStateException("Storage connector is in an unexpected state. There's a bug in concurrency control");
        }
    }

    public Optional<Path> getPath() { return getSilently(Path.class); }

    public Optional<URI> getURI() { return getSilently(URI.class); }

    public Optional<DataSource> getSQLDatasource() { return getSilently(DataSource.class); }

    public Optional<String> getPathAsString() { return getSilently(String.class); }

    final boolean prefetch() throws DataStoreException {
        try {
            return doUnderControl(storage::prefetch);
        } catch (IOException e) {
            throw new DataStoreException("IO error while trying to fetch more content", e);
        }
    }

    /**
     * Retrieve storage in the queried form, closing all other opened view in the same time.
     * <em>Warning</em>: This method also closes this storage connector, making invalid any more calls on it.
     *
     * @param target Type of the view to get back / keep opened.
     * @param <T> Type of the wanted storage connection.
     * @return Underlying storage in the requested form. Never null.
     *
     * @throws DataStoreException If this connector is used concurrently, or if any problem occurs while initializing view.
     * @throws IllegalStateException If this connector is already closed.
     */
    public <T> T commit(Class<T> target) throws DataStoreException {
        return commit(target, null, null);
    }

    public <T> T commit(Class<T> target, String caller, Locale errorLocale) throws DataStoreException {
        try {
            return doUnderControl(() -> {
                final T result = getOrFail(target, caller, errorLocale);
                stateFlag.set(-1); //close flag
                storage.closeAllExcept(result);
                return result;
            });
        } catch (IOException e) {
            throw new DataStoreException(e); // TODO: define a better handling
        }
    }

    private <T> T getOrFail(Class<T> target, String caller, Locale errorLocale) throws DataStoreException {
        T view = storage.getStorageAs(target);
        if (view == null) throw unsupported(target, errorLocale, caller);
        return view;
    }

    private <T> Optional<T> getSilently(Class<T> target) {
        try {
            return Optional.ofNullable(storage.getStorageAs(target));
        } catch (UnconvertibleObjectException e) {
            // TODO: log fine
            return Optional.empty();
        } catch (DataStoreException e) {
            // According to current implementation, that should never happen.
            // Moreover, it is not really logic to propagate DataStoreException, as this operation should not involve
            // any "storage" logic (only in-memory path/uri conversion if needed).
            throw new BackingStoreException(e);
        }
    }

    @Override
    public void close() throws DataStoreException {
        closeAllExcept(committedStorage == null ? null : committedStorage.get());
    }

    public <T> T getOption(OptionKey<T> key) {
        return storage.getOption(key);
    }

    public <T> void setOption(final OptionKey<T> key, final T value) {
        storage.setOption(key, value);
    }

    public String getStorageName() {
        return storage.getStorageName();
    }

    public String getFileExtension() {
        return storage.getFileExtension();
    }

    /**
     * @deprecated Please use a safe alternative instead, like {@link #useAs(Class, StorageOperatingFunction)}.
     * If safe methods do not support the storage type you require, use {@code unsafe().getStorageAs() } to ease tracing
     * of unsafe calls.
     * @return A connector that does not provides control over resource usage. It does not automatically rewind
     * automatically used resources. That means that resource lifecycle becomes <em>entirely</em> caller responsability.
     */
    @Deprecated
    public <T> T getStorageAs(final Class<T> type) throws IllegalArgumentException, DataStoreException {
        return storage.getStorageAs(type);
    }

    /**
     * @deprecated Please use a safe alternative instead, like {@link #useAs(Class, StorageOperatingFunction)}.
     * If safe methods do not support the storage type you require, use {@code unsafe().getStorage() } to ease tracing
     * of unsafe calls.
     *
     * @return A connector that does not provides control over resource usage. It does not automatically rewind
     * automatically used resources. That means that resource lifecycle becomes <em>entirely</em> caller responsability.
     */
    @Deprecated
    public Object getStorage() throws DataStoreException {
        return storage.getStorage();
    }

    public UnsupportedStorageException unsupported(final Locale errorLocale, final String caller) {
        return unsupported(unsafe().type(), errorLocale, caller);
    }

    UnsupportedStorageException unsupported(final Object storage, final Locale errorLocale, final String caller) {
        return new UnsupportedStorageException(
                errorLocale == null ? Locale.ROOT : errorLocale,
                caller == null ? "Unidentified caller" : caller,
                storage,
                getOption(OptionKey.OPEN_OPTIONS)
        );
    }

    /**
     * Please try to use {@link #useAs(Class, StorageOperatingFunction)} instead. Only call this if safe methods do not
     * support the storage type you require.
     *
     * @return A connector that does not provides control over resource usage. It does not automatically rewind
     * automatically used resources. That means that resource lifecycle becomes <em>entirely</em> caller responsability.
     */
    public UnsafeConnector unsafe() {
        return storage;
    }

    @Override
    public String toString() {
        return storage.toString();
    }

    private interface StorageCallable<V> extends Callable<V> {
        @Override
        V call() throws IOException, DataStoreException;
    }

    @FunctionalInterface
    public interface StorageOperatingFunction<I, O> {
        O apply(I storage) throws IOException, DataStoreException;

        default O applyCatchingIO(I storage) throws DataStoreException {
            try {
                return apply(storage);
            } catch (IOException e) {
                throw new DataStoreException(e);
            }
        }
    }

    /**
     * indicates a failure over resource health control (rewind after use, etc.);
     */
    public static class StorageControlException extends RuntimeException {
        public final Class<?> storageType;

        public StorageControlException(Class<?> storageType) {
            this(null, null, storageType);
        }

        public StorageControlException(String message, Class<?> storageType) {
            this(message, null, storageType);
        }

        public StorageControlException(Throwable cause, Class<?> storageType) {
            this(null, cause, storageType);
        }

        public StorageControlException(String message, Throwable cause, Class<?> storageType) {
            super(message, cause);
            this.storageType = storageType;
        }
    }

    interface ControlStrategy<S> {
        Class<S> getType();

        ControlOperator<S> init(S storage) throws IOException;
    }

    interface ControlOperator<S> {
        S getStorage();
        void postControl() throws StorageControlException, IOException;
    }

    private static class ByteBufferControl implements ControlStrategy<ByteBuffer> {

        @Override
        public Class<ByteBuffer> getType() {
            return ByteBuffer.class;
        }

        @Override
        public ControlOperator<ByteBuffer> init(ByteBuffer storage) {
            final ByteBuffer freezed = storage.asReadOnlyBuffer();
            return new ControlOperator<ByteBuffer>() {
                @Override
                public ByteBuffer getStorage() {
                    return freezed;
                }

                @Override
                public void postControl() throws StorageControlException {
                    storage.rewind();
                }
            };
        }
    }

    private static class StreamControl<T> implements ControlStrategy<T> {

        private final Class<T> viewType;
        private final Function<T, ResetableStream> adapter;

        public StreamControl(Class<T> viewType, Function<T, ResetableStream> adapter) {
            this.viewType = viewType;
            this.adapter = adapter;
        }

        @Override
        public Class<T> getType() {
            return viewType;
        }

        @Override
        public ControlOperator<T> init(T storage) throws IOException {
            final ResetableStream adaptedStorage = adapter.apply(storage);
            final long positionCtrl = adaptedStorage.mark();

            return new ControlOperator<T>() {
                @Override
                public T getStorage() {
                    return storage;
                }

                @Override
                public void postControl() throws StorageControlException, IOException {
                    adaptedStorage.reset();

                    final long rewindPosition = adaptedStorage.mark();
                    if (rewindPosition != positionCtrl) {
                        throw new StorageControlException(String.format(
                                "Operator has messed with stream marks. Rewind should have positioned at %d, but ended at %d",
                                positionCtrl, rewindPosition
                        ), viewType);
                    }
                }
            };
        }
    }

    private interface ResetableStream {
        long mark() throws IOException;
        void reset() throws IOException;
    }

    private static class IISAdapter implements ResetableStream {
        private final ImageInputStream source;

        IISAdapter(ImageInputStream source) {
            this.source = source;
        }

        @Override
        public long mark() throws IOException {
            source.mark();
            return source.getStreamPosition();
        }

        @Override
        public void reset() throws IOException {
            source.reset();
        }
    }

    private static class CDIAdapter implements ResetableStream {
        private final ChannelDataInput source;

        CDIAdapter(ChannelDataInput source) {
            this.source = source;
        }

        @Override
        public long mark() {
            source.mark();
            return source.getStreamPosition();
        }

        @Override
        public void reset() throws IOException {
            source.reset();
        }
    }

    private static class SBCAdapter implements ResetableStream {
        final SeekableByteChannel source;

        long mark = -1;

        SBCAdapter(SeekableByteChannel source) {
            this.source = source;
        }

        @Override
        public long mark() throws IOException {
            final long position = source.position();
            if (mark >= 0 && position != mark) throw new IllegalStateException("This stream is already marked, cannot add one.");
            this.mark = position;
            return mark;
        }

        @Override
        public void reset() throws IOException {
            if (mark >= 0) source.position(mark);
        }
    }

    public interface Result<O> {

        <I> Result<O> orTryUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) throws DataStoreException;

        <I> O orUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) throws DataStoreException;

        O orElse(O value);

        default O getOrThrow() throws UnsupportedStorageException {
            return getOrThrow(null, "Unidentified");
        }

        O getOrThrow(Locale errorLocale, String caller) throws UnsupportedStorageException;

        <T extends Throwable> O getOrThrow(Supplier<T> errorSupplier) throws T;
    }

    private static final class Success<O> implements Result<O> {

        private final O value;

        private Success(O value) {
            this.value = value;
        }

        @Override
        public <I> Result<O> orTryUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) {
            return this;
        }

        @Override
        public <I> O orUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) {
            return value;
        }

        @Override
        public O orElse(O value) {
            return this.value;
        }

        @Override
        public O getOrThrow(Locale errorLocale, String caller) {
            return value;
        }

        @Override
        public <T extends Throwable> O getOrThrow(Supplier<T> errorSupplier) {
            return value;
        }
    }

    private final class Failure<O> implements Result<O> {

        private final String optMessage;

        private Failure() { this(null); }
        private Failure(String optMessage) {
            this.optMessage = optMessage;
        }

        @Override
        public <I> Result<O> orTryUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) throws DataStoreException {
            return tryUseAs(storageType, operator);
        }

        @Override
        public <I> O orUseAs(Class<I> storageType, StorageOperatingFunction<? super I, ? extends O> operator) throws DataStoreException {
            return useAs(storageType, operator);
        }

        @Override
        public O orElse(O value) {
            return value;
        }

        @Override
        public O getOrThrow() throws UnsupportedStorageException {
            if (optMessage != null) throw new UnsupportedStorageException(optMessage);
            else return getOrThrow(null, "Unidentified");
        }

        @Override
        public O getOrThrow(Locale errorLocale, String caller) throws UnsupportedStorageException {
            throw unsupported(errorLocale, caller);
        }

        @Override
        public <T extends Throwable> O getOrThrow(Supplier<T> errorSupplier) throws T {
            throw errorSupplier.get();
        }
    }
}
