package org.apache.sis.storage.netcdf.zarr;

import org.apache.sis.setup.GeometryLibrary;
import org.apache.sis.storage.DataStoreException;
import org.apache.sis.storage.Resource;
import org.apache.sis.storage.StorageConnector;
import org.apache.sis.storage.netcdf.NetcdfStore;
import org.apache.sis.storage.netcdf.NetcdfStoreProviderTest;
import org.apache.sis.storage.netcdf.base.Decoder;
import org.apache.sis.storage.netcdf.base.DecoderTest;
import org.apache.sis.storage.netcdf.base.Encoder;
import org.apache.sis.storage.netcdf.base.TestCase;
import org.junit.jupiter.api.Test;
import org.opengis.test.dataset.TestData;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.Collection;

import static org.apache.sis.storage.netcdf.AttributeNames.CONTRIBUTOR;
import static org.apache.sis.storage.netcdf.AttributeNames.CREATOR;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_CREATED;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_ISSUED;
import static org.apache.sis.storage.netcdf.AttributeNames.DATE_MODIFIED;
import static org.apache.sis.storage.netcdf.AttributeNames.LATITUDE;
import static org.apache.sis.storage.netcdf.AttributeNames.LONGITUDE;
import static org.apache.sis.storage.netcdf.AttributeNames.SUMMARY;
import static org.apache.sis.storage.netcdf.AttributeNames.TITLE;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Tests the {@link ZarrDecoder} implementation.
 *
 * @author  Quentin Bialota (Geomatys)
 */
public class ZarrDecoderTest extends TestCase {
    /**
     * Creates a new test case.
     */
    public ZarrDecoderTest() {
    }

    /**
     * Creates a new decoder for the specified dataset.
     *
     * @return the decoder for the specified dataset.
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Override
    protected Decoder createDecoder(final TestData file) throws IOException, DataStoreException {
        return createZarrDecoder(file.file().toPath());
    }

    public static Decoder createZarrDecoder(final Path file) throws IOException, DataStoreException {
        return new ZarrDecoder(file, GeometryLibrary.JAVA2D, createListeners());
    }

    public static Path getPath(String filename) throws DataStoreException, IOException {
        try {
            return Paths.get(NetcdfStoreProviderTest.class.getResource(filename).toURI());
        } catch (URISyntaxException e) {
            throw new DataStoreException(e);
        }
    }

    /**
     * Tests {@link Decoder#stringValue(String)} with global attributes.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testStringValue() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));

        assertEquals("hours since 2013-01-01 00:00:00", decoder.stringValue("units"), "unis");
        assertEquals("proleptic_gregorian", decoder.stringValue("calendar"), "calendar");
    }

    /**
     * Tests {@link Decoder#numberToDate(String, Number[])}.
     *
     * @throws IOException if an I/O error occurred while opening the file.
     * @throws DataStoreException if a logical error occurred.
     */
    @Test
    public void testNumberToDate() throws IOException, DataStoreException {
        final Decoder decoder = createZarrDecoder(getPath("/org/apache/sis/storage/netcdf/resources/zarr/dggs.zarr"));
        assertArrayEquals(new Instant[] {
                Instant.parse("2005-09-22T00:00:00Z")
        }, decoder.numberToDate("hours since 1992-1-1", 120312));

        assertArrayEquals(new Instant[] {
                Instant.parse("1970-01-09T18:00:00Z"),
                Instant.parse("1969-12-29T06:00:00Z"),
                Instant.parse("1993-04-10T00:00:00Z")
        }, decoder.numberToDate("days since 1970-01-01T00:00:00Z", 8.75, -2.75, 8500));
    }
}
