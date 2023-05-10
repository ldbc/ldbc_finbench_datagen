package ldbc.finbench.datagen.util;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;

public class DateTimeUtils {
    public static ZoneId UTC = ZoneId.of("UTC");

    public static long toEpochMilli(LocalDate ld) {
        return ld.atStartOfDay(UTC).toInstant().toEpochMilli();
    }

    public static long toEpochMilli(LocalDateTime ldt) {
        return ldt.atZone(UTC).toInstant().toEpochMilli();
    }

    public static LocalDate utcDateOfEpochMilli(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(UTC).toLocalDate();
    }

    public static LocalDateTime utcDateTimeOfEpochMilli(long epochMilli) {
        return Instant.ofEpochMilli(epochMilli).atZone(UTC).toLocalDateTime();
    }

    public static boolean isTravelSeason(long epochMilli) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);

        int day = date.getDayOfMonth();
        int month = date.getMonthValue();

        if ((month > 4) && (month < 7)) {
            return true;
        }
        return ((month == 11) && (day > 23));
    }

    public static int getNumberOfMonths(long epochMilli, int startMonth, int startYear) {
        LocalDate date = utcDateOfEpochMilli(epochMilli);
        int month = date.getMonthValue();
        int year = date.getYear();
        return (year - startYear) * 12 + month - (startMonth - 1);
    }

    public static int getYear(long epochMilli) {
        LocalDateTime datetime = utcDateTimeOfEpochMilli(epochMilli);
        return datetime.getYear();
    }

    public static Month getMonth(long epochMilli) {
        LocalDateTime datetime = utcDateTimeOfEpochMilli(epochMilli);
        return datetime.getMonth();
    }
}
