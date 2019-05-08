package com.boraydata.utils;

import java.util.Arrays;
import java.util.Random;

public final class StringUtils {

    private static final char[] HEX_CHARS = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

    /**
     * Given an array of bytes it will convert the bytes to a hex string
     * representation of the bytes.
     *
     * @param bytes
     *        the bytes to convert in a hex string
     * @param start
     *        start index, inclusively
     * @param end
     *        end index, exclusively
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes, final int start, final int end) {
        if (bytes == null) {
            throw new IllegalArgumentException("bytes == null");
        }



        int length = end - start;
        char[] out = new char[length * 2];

        for (int i = start, j = 0; i < end; i++) {
            out[j++] = HEX_CHARS[(0xF0 & bytes[i]) >>> 4];
            out[j++] = HEX_CHARS[0x0F & bytes[i]];
        }

        return new String(out);
    }

    /**
     * Given an array of bytes it will convert the bytes to a hex string
     * representation of the bytes.
     *
     * @param bytes
     *        the bytes to convert in a hex string
     * @return hex string representation of the byte array
     */
    public static String byteToHexString(final byte[] bytes) {
        return byteToHexString(bytes, 0, bytes.length);
    }

    /**
     * Given a hex string this will return the byte array corresponding to the
     * string .
     *
     * @param hex
     *        the hex String array
     * @return a byte array that is a hex string representation of the given
     *         string. The size of the byte array is therefore hex.length/2
     */
    public static byte[] hexStringToByte(final String hex) {
        final byte[] bts = new byte[hex.length() / 2];
        for (int i = 0; i < bts.length; i++) {
            bts[i] = (byte) Integer.parseInt(hex.substring(2 * i, 2 * i + 2), 16);
        }
        return bts;
    }

    /**
     * This method calls {@link Object#toString()} on the given object, unless the
     * object is an array. In that case, it will use the {@link #arrayToString(Object)}
     * method to create a string representation of the array that includes all contained
     * elements.
     *
     * @param o The object for which to create the string representation.
     * @return The string representation of the object.
     */
    public static String arrayAwareToString(Object o) {
        if (o == null) {
            return "null";
        }
        if (o.getClass().isArray()) {
            return arrayToString(o);
        }

        return o.toString();
    }

    /**
     * Returns a string representation of the given array. This method takes an Object
     * to allow also all types of primitive type arrays.
     *
     * @param array The array to create a string representation for.
     * @return The string representation of the array.
     * @throws IllegalArgumentException If the given object is no array.
     */
    public static String arrayToString(Object array) {
        if (array == null) {
            throw new NullPointerException();
        }

        if (array instanceof int[]) {
            return Arrays.toString((int[]) array);
        }
        if (array instanceof long[]) {
            return Arrays.toString((long[]) array);
        }
        if (array instanceof Object[]) {
            return Arrays.toString((Object[]) array);
        }
        if (array instanceof byte[]) {
            return Arrays.toString((byte[]) array);
        }
        if (array instanceof double[]) {
            return Arrays.toString((double[]) array);
        }
        if (array instanceof float[]) {
            return Arrays.toString((float[]) array);
        }
        if (array instanceof boolean[]) {
            return Arrays.toString((boolean[]) array);
        }
        if (array instanceof char[]) {
            return Arrays.toString((char[]) array);
        }
        if (array instanceof short[]) {
            return Arrays.toString((short[]) array);
        }

        if (array.getClass().isArray()) {
            return "<unknown array type>";
        } else {
            throw new IllegalArgumentException("The given argument is no array.");
        }
    }

    /**
     * Replaces control characters by their escape-coded version. For example,
     * if the string contains a line break character ('\n'), this character will
     * be replaced by the two characters backslash '\' and 'n'. As a consequence, the
     * resulting string will not contain any more control characters.
     *
     * @param str The string in which to replace the control characters.
     * @return The string with the replaced characters.
     */
    public static String showControlCharacters(String str) {
        int len = str.length();
        StringBuilder sb = new StringBuilder();

        for (int i = 0; i < len; i += 1) {
            char c = str.charAt(i);
            switch (c) {
                case '\b':
                    sb.append("\\b");
                    break;
                case '\t':
                    sb.append("\\t");
                    break;
                case '\n':
                    sb.append("\\n");
                    break;
                case '\f':
                    sb.append("\\f");
                    break;
                case '\r':
                    sb.append("\\r");
                    break;
                default:
                    sb.append(c);
            }
        }

        return sb.toString();
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only characters that
     * can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @return A random String.
     */
    public static String getRandomString(Random rnd, int minLength, int maxLength) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(0x7fff) + 1);
        }
        return new String(data);
    }

    /**
     * Creates a random string with a length within the given interval. The string contains only characters that
     * can be represented as a single code point.
     *
     * @param rnd The random used to create the strings.
     * @param minLength The minimum string length.
     * @param maxLength The maximum string length (inclusive).
     * @param minValue The minimum character value to occur.
     * @param maxValue The maximum character value to occur.
     * @return A random String.
     */
    public static String getRandomString(Random rnd, int minLength, int maxLength, char minValue, char maxValue) {
        int len = rnd.nextInt(maxLength - minLength + 1) + minLength;

        char[] data = new char[len];
        int diff = maxValue - minValue + 1;

        for (int i = 0; i < data.length; i++) {
            data[i] = (char) (rnd.nextInt(diff) + minValue);
        }
        return new String(data);
    }






    /**
     * Checks if the string is null, empty, or contains only whitespace characters.
     * A whitespace character is defined via {@link Character#isWhitespace(char)}.
     *
     * @param str The string to check
     * @return True, if the string is null or blank, false otherwise.
     */
    public static boolean isNullOrWhitespaceOnly(String str) {
        if (str == null || str.length() == 0) {
            return true;
        }

        final int len = str.length();
        for (int i = 0; i < len; i++) {
            if (!Character.isWhitespace(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    /**
     * If both string arguments are non-null, this method concatenates them with ' and '.
     * If only one of the arguments is non-null, this method returns the non-null argument.
     * If both arguments are null, this method returns null.
     *
     * @param s1 The first string argument
     * @param s2 The second string argument
     *
     * @return The concatenated string, or non-null argument, or null 
     */
    
    public static String concatenateWithAnd( String s1, String s2) {
        if (s1 != null) {
            return s2 == null ? s1 : s1 + " and " + s2;
        }
        else {
            return s2 != null ? s2 : null;
        }
    }

    // ------------------------------------------------------------------------

    /** Prevent instantiation of this utility class */
    private StringUtils() {}
}
