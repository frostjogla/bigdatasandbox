package ru.fj.custominputformat;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

import org.apache.hadoop.io.Text;
import org.junit.Test;

import ru.fj.custominputformat.CsvRecordReader;

public class CsvRecordReaderTest {

    private final static String LINE_GROUPED_BY_2 = "ASD,12,LFKG,99,KDLD,32,YTRKI,43,ieru,f";
    private final static String LINE_GROUPED_BY_3_1 = "ASD,m,12,LFKG,f,99,KDLD,f,32,YTRKI,f,43,ieru,f";
    private final static String LINE_GROUPED_BY_3_2 = "UIE,m,13,LOOP,f,99,EKWEYR,f,32,YTRKI,f,43,IERU,f";

    private CsvRecordReader csvRecordReader;

    @Test
    public void nextKeyValueTestGroupedBy3() throws Exception {
        csvRecordReader = spy(new CsvRecordReader(",", 3));

        doReturn(true).doReturn(true).when(csvRecordReader).nextKeyValueWrapper();
        doReturn(new Text(LINE_GROUPED_BY_3_1)).doReturn(new Text(LINE_GROUPED_BY_3_2)).when(csvRecordReader)
                .getCurrentValueWrapper();
        csvRecordReader.nextKeyValue();
        assertEquals("ASD,m,12", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("LFKG,f,99", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("KDLD,f,32", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("YTRKI,f,43", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("ieru,f", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("UIE,m,13", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("LOOP,f,99", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("EKWEYR,f,32", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("YTRKI,f,43", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("IERU,f", csvRecordReader.getCurrentValue().toString());
    }

    @Test
    public void nextKeyValueTestGroupedBy2() throws Exception {
        csvRecordReader = spy(new CsvRecordReader(",", 2));
        doReturn(true).when(csvRecordReader).nextKeyValueWrapper();
        doReturn(new Text(LINE_GROUPED_BY_2)).when(csvRecordReader).getCurrentValueWrapper();

        csvRecordReader.nextKeyValue();
        assertEquals("ASD,12", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("LFKG,99", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("KDLD,32", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("YTRKI,43", csvRecordReader.getCurrentValue().toString());
        csvRecordReader.nextKeyValue();
        assertEquals("ieru,f", csvRecordReader.getCurrentValue().toString());
    }

}
