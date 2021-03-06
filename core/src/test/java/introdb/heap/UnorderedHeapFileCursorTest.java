package introdb.heap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import introdb.heap.UnorderedHeapFile.Cursor;

class UnorderedHeapFileCursorTest {

    private UnorderedHeapFile heapFile;

    @BeforeEach
    void setUp() throws IOException {
        Path heapFilePath = Files.createTempFile("heap", "0001");
        heapFile = new UnorderedHeapFile(heapFilePath, 1024, 4 * 1024);
    }

    @Test
    void does_not_has_next_on_empty_file() {

        // when
        Cursor cursor = heapFile.cursor();
        boolean hasNext = cursor.hasNext();

        // then
        assertThat(hasNext).isFalse();

    }

    @ParameterizedTest
    @ValueSource(ints = {512,2048})
    void iterate_over_records(int valueSize) throws IOException, ClassNotFoundException {
        // given
        var firstkey = "1";
        var firstvalue = putRandomValue(firstkey, valueSize);

        var secondkey = "2";
        var secondvalue = putRandomValue(secondkey, valueSize);

        // when
        Cursor cursor = heapFile.cursor();
        var hasNext = cursor.hasNext();
        // then
        assertThat(hasNext).isTrue();

        // when
        var record = cursor.next();
        // then
        assertThat(record).isEqualTo(Record.of(newEntry(firstkey, firstvalue)));

        // when
        hasNext = cursor.hasNext();
        // then
        assertThat(hasNext).isTrue();

        // when
        record = cursor.next();
        // then
        assertThat(record).isEqualTo(Record.of(newEntry(secondkey, secondvalue)));

        assertThat(cursor.hasNext()).isFalse();
        assertThatThrownBy(cursor::next).isInstanceOf(NoSuchElementException.class);

    }

    private byte[] putRandomValue(String key, int valueSize) throws IOException, ClassNotFoundException {
        var value = new byte[valueSize];
        new Random().nextBytes(value);
        heapFile.put(newEntry(key, value));
        return value;
    }

    private Entry newEntry(Serializable firstkey, Serializable firstvalue) {
        return new Entry(firstkey, firstvalue);
    }

}