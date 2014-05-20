package com.nexr.platform.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * UDFAddMonths
 */
public class UDFAddMonths extends UDF {
    private final SimpleDateFormat stdFormatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private final Calendar calendar = Calendar.getInstance();

    public UDFAddMonths() {
        stdFormatter.setLenient(false);
    }

    Text result= new Text();

    public Text evaluate(Text dateText, IntWritable months) {
        if (dateText == null || months == null) {
            return null;
        }

        try {
            Date date = stdFormatter.parse(dateText.toString());
            calendar.setTime(date);
            calendar.add(Calendar.MONTH, months.get());
            Date newDate = calendar.getTime();
            result.set(stdFormatter.format(newDate));
        } catch (ParseException e) {
            return null;
        }

        return result;
    }
}
