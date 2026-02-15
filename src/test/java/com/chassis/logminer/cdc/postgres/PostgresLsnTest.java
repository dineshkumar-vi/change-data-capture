package com.chassis.logminer.cdc.postgres;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Comprehensive unit tests for PostgresLsn class.
 * Tests all public methods including edge cases, null handling, and boundary conditions.
 */
class PostgresLsnTest {

    // ==================== Constructor Tests ====================

    @Nested
    @DisplayName("Constructor Tests")
    class ConstructorTests {

        @Test
        @DisplayName("Should create PostgresLsn with valid BigInteger")
        void testConstructorWithValidBigInteger() {
            BigInteger value = BigInteger.valueOf(100);
            PostgresLsn lsn = new PostgresLsn(value);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(100L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn with null value")
        void testConstructorWithNull() {
            PostgresLsn lsn = new PostgresLsn(null);

            assertNotNull(lsn);
            assertTrue(lsn.isNull());
            assertEquals(0L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn with zero")
        void testConstructorWithZero() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.ZERO);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(0L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn with negative value")
        void testConstructorWithNegativeValue() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.valueOf(-100));

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(-100L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn with large value")
        void testConstructorWithLargeValue() {
            BigInteger largeValue = new BigInteger("999999999999999999");
            PostgresLsn lsn = new PostgresLsn(largeValue);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(999999999999999999L, lsn.longValue());
        }
    }

    // ==================== Constant Tests ====================

    @Nested
    @DisplayName("Constant Tests")
    class ConstantTests {

        @Test
        @DisplayName("MAX constant should have value -2")
        void testMaxConstant() {
            assertNotNull(PostgresLsn.MAX);
            assertFalse(PostgresLsn.MAX.isNull());
            assertEquals(-2L, PostgresLsn.MAX.longValue());
        }

        @Test
        @DisplayName("NULL constant should be null")
        void testNullConstant() {
            assertNotNull(PostgresLsn.NULL);
            assertTrue(PostgresLsn.NULL.isNull());
            assertEquals(0L, PostgresLsn.NULL.longValue());
        }

        @Test
        @DisplayName("ONE constant should have value 1")
        void testOneConstant() {
            assertNotNull(PostgresLsn.ONE);
            assertFalse(PostgresLsn.ONE.isNull());
            assertEquals(1L, PostgresLsn.ONE.longValue());
        }
    }

    // ==================== isNull() Tests ====================

    @Nested
    @DisplayName("isNull() Tests")
    class IsNullTests {

        @Test
        @DisplayName("Should return true for null LSN")
        void testIsNullReturnsTrueForNull() {
            PostgresLsn lsn = new PostgresLsn(null);
            assertTrue(lsn.isNull());
        }

        @Test
        @DisplayName("Should return false for non-null LSN")
        void testIsNullReturnsFalseForNonNull() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.valueOf(100));
            assertFalse(lsn.isNull());
        }

        @Test
        @DisplayName("Should return false for zero LSN")
        void testIsNullReturnsFalseForZero() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.ZERO);
            assertFalse(lsn.isNull());
        }
    }

    // ==================== valueOf() Tests ====================

    @Nested
    @DisplayName("valueOf() Factory Method Tests")
    class ValueOfTests {

        @Test
        @DisplayName("Should create PostgresLsn from positive int")
        void testValueOfPositiveInt() {
            PostgresLsn lsn = PostgresLsn.valueOf(42);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(42L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from negative int")
        void testValueOfNegativeInt() {
            PostgresLsn lsn = PostgresLsn.valueOf(-42);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(-42L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from zero int")
        void testValueOfZeroInt() {
            PostgresLsn lsn = PostgresLsn.valueOf(0);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(0L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from positive long")
        void testValueOfPositiveLong() {
            PostgresLsn lsn = PostgresLsn.valueOf(1234567890L);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(1234567890L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from negative long")
        void testValueOfNegativeLong() {
            PostgresLsn lsn = PostgresLsn.valueOf(-1234567890L);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(-1234567890L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from max long value")
        void testValueOfMaxLong() {
            PostgresLsn lsn = PostgresLsn.valueOf(Long.MAX_VALUE);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(Long.MAX_VALUE, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from min long value")
        void testValueOfMinLong() {
            PostgresLsn lsn = PostgresLsn.valueOf(Long.MIN_VALUE);

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(Long.MIN_VALUE, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from numeric string")
        void testValueOfNumericString() {
            PostgresLsn lsn = PostgresLsn.valueOf("12345");

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(12345L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from negative numeric string")
        void testValueOfNegativeString() {
            PostgresLsn lsn = PostgresLsn.valueOf("-12345");

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(-12345L, lsn.longValue());
        }

        @Test
        @DisplayName("Should create PostgresLsn from large numeric string")
        void testValueOfLargeString() {
            PostgresLsn lsn = PostgresLsn.valueOf("999999999999999999");

            assertNotNull(lsn);
            assertFalse(lsn.isNull());
            assertEquals(999999999999999999L, lsn.longValue());
        }

        @Test
        @DisplayName("Should throw exception for invalid string")
        void testValueOfInvalidString() {
            assertThrows(NumberFormatException.class, () -> {
                PostgresLsn.valueOf("invalid");
            });
        }

        @Test
        @DisplayName("Should throw exception for null string")
        void testValueOfNullString() {
            assertThrows(NullPointerException.class, () -> {
                PostgresLsn.valueOf((String) null);
            });
        }

        @Test
        @DisplayName("Should throw exception for empty string")
        void testValueOfEmptyString() {
            assertThrows(NumberFormatException.class, () -> {
                PostgresLsn.valueOf("");
            });
        }
    }

    // ==================== longValue() Tests ====================

    @Nested
    @DisplayName("longValue() Tests")
    class LongValueTests {

        @Test
        @DisplayName("Should return correct long value")
        void testLongValueReturnsCorrectValue() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.valueOf(12345));
            assertEquals(12345L, lsn.longValue());
        }

        @Test
        @DisplayName("Should return 0 for null LSN")
        void testLongValueReturnsZeroForNull() {
            PostgresLsn lsn = new PostgresLsn(null);
            assertEquals(0L, lsn.longValue());
        }

        @Test
        @DisplayName("Should return 0 for zero LSN")
        void testLongValueReturnsZeroForZero() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.ZERO);
            assertEquals(0L, lsn.longValue());
        }

        @Test
        @DisplayName("Should return negative long value")
        void testLongValueReturnsNegative() {
            PostgresLsn lsn = new PostgresLsn(BigInteger.valueOf(-999));
            assertEquals(-999L, lsn.longValue());
        }
    }

    // ==================== add() Tests ====================

    @Nested
    @DisplayName("add() Tests")
    class AddTests {

        @Test
        @DisplayName("Should add two positive LSNs")
        void testAddPositiveLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(50);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(150L, result.longValue());
        }

        @Test
        @DisplayName("Should add positive and negative LSNs")
        void testAddPositiveAndNegative() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-30);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(70L, result.longValue());
        }

        @Test
        @DisplayName("Should add two negative LSNs")
        void testAddNegativeLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(-100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-50);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(-150L, result.longValue());
        }

        @Test
        @DisplayName("Should add LSN with zero")
        void testAddWithZero() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(0);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(100L, result.longValue());
        }

        @Test
        @DisplayName("Should return NULL when both LSNs are null")
        void testAddBothNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.NULL;

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertTrue(result.isNull());
        }

        @Test
        @DisplayName("Should return copy of first LSN when second is null")
        void testAddFirstNotNullSecondNull() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.NULL;

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(100L, result.longValue());
        }

        @Test
        @DisplayName("Should return copy of second LSN when first is null")
        void testAddFirstNullSecondNotNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(100L, result.longValue());
        }

        @Test
        @DisplayName("Should handle large values in addition")
        void testAddLargeValues() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(Long.MAX_VALUE / 2);
            PostgresLsn lsn2 = PostgresLsn.valueOf(Long.MAX_VALUE / 2);

            PostgresLsn result = lsn1.add(lsn2);

            assertNotNull(result);
            assertEquals(Long.MAX_VALUE - 1, result.longValue());
        }
    }

    // ==================== subtract() Tests ====================

    @Nested
    @DisplayName("subtract() Tests")
    class SubtractTests {

        @Test
        @DisplayName("Should subtract two positive LSNs")
        void testSubtractPositiveLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(30);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(70L, result.longValue());
        }

        @Test
        @DisplayName("Should subtract resulting in negative")
        void testSubtractResultingNegative() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(50);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(-50L, result.longValue());
        }

        @Test
        @DisplayName("Should subtract negative LSN (adding)")
        void testSubtractNegativeLsn() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-30);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(130L, result.longValue());
        }

        @Test
        @DisplayName("Should subtract two negative LSNs")
        void testSubtractNegativeLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(-100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-50);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(-50L, result.longValue());
        }

        @Test
        @DisplayName("Should subtract zero resulting in same value")
        void testSubtractZero() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(0);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(100L, result.longValue());
        }

        @Test
        @DisplayName("Should return NULL when both LSNs are null")
        void testSubtractBothNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.NULL;

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertTrue(result.isNull());
        }

        @Test
        @DisplayName("Should return copy of first LSN when second is null")
        void testSubtractFirstNotNullSecondNull() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.NULL;

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(100L, result.longValue());
        }

        @Test
        @DisplayName("Should return negated second LSN when first is null")
        void testSubtractFirstNullSecondNotNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(-100L, result.longValue());
        }

        @Test
        @DisplayName("Should handle large values in subtraction")
        void testSubtractLargeValues() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(Long.MAX_VALUE);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            PostgresLsn result = lsn1.subtract(lsn2);

            assertNotNull(result);
            assertEquals(Long.MAX_VALUE - 100, result.longValue());
        }
    }

    // ==================== compareTo() Tests ====================

    @Nested
    @DisplayName("compareTo() Tests")
    class CompareToTests {

        @Test
        @DisplayName("Should return 0 for equal LSNs")
        void testCompareToEqual() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertEquals(0, lsn1.compareTo(lsn2));
        }

        @Test
        @DisplayName("Should return negative when first is less than second")
        void testCompareToLessThan() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(50);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertTrue(lsn1.compareTo(lsn2) < 0);
        }

        @Test
        @DisplayName("Should return positive when first is greater than second")
        void testCompareToGreaterThan() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(50);

            assertTrue(lsn1.compareTo(lsn2) > 0);
        }

        @Test
        @DisplayName("Should return 0 when both LSNs are null")
        void testCompareToBothNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.NULL;

            assertEquals(0, lsn1.compareTo(lsn2));
        }

        @Test
        @DisplayName("Should return -1 when first is null and second is not")
        void testCompareToFirstNullSecondNotNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertEquals(-1, lsn1.compareTo(lsn2));
        }

        @Test
        @DisplayName("Should return 1 when first is not null and second is null")
        void testCompareToFirstNotNullSecondNull() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.NULL;

            assertEquals(1, lsn1.compareTo(lsn2));
        }

        @Test
        @DisplayName("Should compare negative LSNs correctly")
        void testCompareToNegativeLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(-100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-50);

            assertTrue(lsn1.compareTo(lsn2) < 0);
        }

        @Test
        @DisplayName("Should compare positive and negative LSNs")
        void testCompareToPositiveAndNegative() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(50);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-50);

            assertTrue(lsn1.compareTo(lsn2) > 0);
        }

        @Test
        @DisplayName("Should compare zero and positive")
        void testCompareToZeroAndPositive() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(0);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertTrue(lsn1.compareTo(lsn2) < 0);
        }

        @Test
        @DisplayName("Should compare large values correctly")
        void testCompareToLargeValues() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(Long.MAX_VALUE);
            PostgresLsn lsn2 = PostgresLsn.valueOf(Long.MAX_VALUE - 1);

            assertTrue(lsn1.compareTo(lsn2) > 0);
        }
    }

    // ==================== equals() Tests ====================

    @Nested
    @DisplayName("equals() Tests")
    class EqualsTests {

        @Test
        @DisplayName("Should return true for same instance")
        void testEqualsSameInstance() {
            PostgresLsn lsn = PostgresLsn.valueOf(100);

            assertTrue(lsn.equals(lsn));
        }

        @Test
        @DisplayName("Should return true for equal LSNs")
        void testEqualsEqualLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertTrue(lsn1.equals(lsn2));
            assertTrue(lsn2.equals(lsn1));
        }

        @Test
        @DisplayName("Should return false for different LSNs")
        void testEqualsDifferentLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(200);

            assertFalse(lsn1.equals(lsn2));
        }

        @Test
        @DisplayName("Should return false for null comparison")
        void testEqualsNull() {
            PostgresLsn lsn = PostgresLsn.valueOf(100);

            assertFalse(lsn.equals(null));
        }

        @Test
        @DisplayName("Should return false for different class")
        void testEqualsDifferentClass() {
            PostgresLsn lsn = PostgresLsn.valueOf(100);
            String other = "100";

            assertFalse(lsn.equals(other));
        }

        @Test
        @DisplayName("Should return true for two null LSNs")
        void testEqualsBothNullLsns() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = new PostgresLsn(null);

            assertTrue(lsn1.equals(lsn2));
        }

        @Test
        @DisplayName("Should return false when one LSN is null and other is not")
        void testEqualsOneNullOneLsnNotNull() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertFalse(lsn1.equals(lsn2));
            assertFalse(lsn2.equals(lsn1));
        }

        @Test
        @DisplayName("Should return true for zero LSNs")
        void testEqualsZeroLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(0);
            PostgresLsn lsn2 = new PostgresLsn(BigInteger.ZERO);

            assertTrue(lsn1.equals(lsn2));
        }

        @Test
        @DisplayName("Should return true for negative equal LSNs")
        void testEqualsNegativeLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(-100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(-100);

            assertTrue(lsn1.equals(lsn2));
        }
    }

    // ==================== hashCode() Tests ====================

    @Nested
    @DisplayName("hashCode() Tests")
    class HashCodeTests {

        @Test
        @DisplayName("Should return same hash code for equal LSNs")
        void testHashCodeEqualLsns() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);

            assertEquals(lsn1.hashCode(), lsn2.hashCode());
        }

        @Test
        @DisplayName("Should return consistent hash code")
        void testHashCodeConsistent() {
            PostgresLsn lsn = PostgresLsn.valueOf(100);

            int hash1 = lsn.hashCode();
            int hash2 = lsn.hashCode();

            assertEquals(hash1, hash2);
        }

        @Test
        @DisplayName("Should return same hash code for null LSNs")
        void testHashCodeNullLsns() {
            PostgresLsn lsn1 = PostgresLsn.NULL;
            PostgresLsn lsn2 = new PostgresLsn(null);

            assertEquals(lsn1.hashCode(), lsn2.hashCode());
        }

        @Test
        @DisplayName("Should handle hash code for zero")
        void testHashCodeZero() {
            PostgresLsn lsn = PostgresLsn.valueOf(0);

            assertNotNull(lsn.hashCode());
        }

        @Test
        @DisplayName("Should handle hash code for negative values")
        void testHashCodeNegative() {
            PostgresLsn lsn = PostgresLsn.valueOf(-100);

            assertNotNull(lsn.hashCode());
        }

        @Test
        @DisplayName("Should handle hash code for large values")
        void testHashCodeLargeValue() {
            PostgresLsn lsn = PostgresLsn.valueOf(Long.MAX_VALUE);

            assertNotNull(lsn.hashCode());
        }
    }

    // ==================== toString() Tests ====================

    @Nested
    @DisplayName("toString() Tests")
    class ToStringTests {

        @Test
        @DisplayName("Should return string representation of positive LSN")
        void testToStringPositive() {
            PostgresLsn lsn = PostgresLsn.valueOf(12345);

            assertEquals("12345", lsn.toString());
        }

        @Test
        @DisplayName("Should return string representation of negative LSN")
        void testToStringNegative() {
            PostgresLsn lsn = PostgresLsn.valueOf(-12345);

            assertEquals("-12345", lsn.toString());
        }

        @Test
        @DisplayName("Should return 'null' for null LSN")
        void testToStringNull() {
            PostgresLsn lsn = PostgresLsn.NULL;

            assertEquals("null", lsn.toString());
        }

        @Test
        @DisplayName("Should return '0' for zero LSN")
        void testToStringZero() {
            PostgresLsn lsn = PostgresLsn.valueOf(0);

            assertEquals("0", lsn.toString());
        }

        @Test
        @DisplayName("Should return string representation of large LSN")
        void testToStringLarge() {
            PostgresLsn lsn = PostgresLsn.valueOf(Long.MAX_VALUE);

            assertEquals(String.valueOf(Long.MAX_VALUE), lsn.toString());
        }

        @Test
        @DisplayName("Should return string representation matching BigInteger toString")
        void testToStringMatchesBigInteger() {
            BigInteger value = new BigInteger("999999999999999999");
            PostgresLsn lsn = new PostgresLsn(value);

            assertEquals(value.toString(), lsn.toString());
        }
    }

    // ==================== Integration/Complex Scenario Tests ====================

    @Nested
    @DisplayName("Integration and Complex Scenario Tests")
    class IntegrationTests {

        @Test
        @DisplayName("Should handle chained operations")
        void testChainedOperations() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(50);
            PostgresLsn lsn3 = PostgresLsn.valueOf(25);

            PostgresLsn result = lsn1.add(lsn2).subtract(lsn3);

            assertEquals(125L, result.longValue());
        }

        @Test
        @DisplayName("Should maintain immutability in operations")
        void testImmutability() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(50);

            PostgresLsn result = lsn1.add(lsn2);

            assertEquals(100L, lsn1.longValue());
            assertEquals(50L, lsn2.longValue());
            assertEquals(150L, result.longValue());
        }

        @Test
        @DisplayName("Should work correctly with constants in operations")
        void testOperationsWithConstants() {
            PostgresLsn result1 = PostgresLsn.ONE.add(PostgresLsn.ONE);
            PostgresLsn result2 = PostgresLsn.MAX.subtract(PostgresLsn.ONE);

            assertEquals(2L, result1.longValue());
            assertEquals(-3L, result2.longValue());
        }

        @Test
        @DisplayName("Should maintain ordering consistency")
        void testOrderingConsistency() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(50);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);
            PostgresLsn lsn3 = PostgresLsn.valueOf(150);

            assertTrue(lsn1.compareTo(lsn2) < 0);
            assertTrue(lsn2.compareTo(lsn3) < 0);
            assertTrue(lsn1.compareTo(lsn3) < 0);
        }

        @Test
        @DisplayName("Should handle mixed null and non-null operations")
        void testMixedNullOperations() {
            PostgresLsn nullLsn = PostgresLsn.NULL;
            PostgresLsn nonNullLsn = PostgresLsn.valueOf(100);

            PostgresLsn addResult = nullLsn.add(nonNullLsn);
            PostgresLsn subtractResult = nullLsn.subtract(nonNullLsn);

            assertEquals(100L, addResult.longValue());
            assertEquals(-100L, subtractResult.longValue());
        }

        @Test
        @DisplayName("Should maintain equals and hashCode contract")
        void testEqualsHashCodeContract() {
            PostgresLsn lsn1 = PostgresLsn.valueOf(100);
            PostgresLsn lsn2 = PostgresLsn.valueOf(100);
            PostgresLsn lsn3 = PostgresLsn.valueOf(100);

            // Reflexive
            assertTrue(lsn1.equals(lsn1));

            // Symmetric
            assertTrue(lsn1.equals(lsn2));
            assertTrue(lsn2.equals(lsn1));

            // Transitive
            assertTrue(lsn1.equals(lsn2));
            assertTrue(lsn2.equals(lsn3));
            assertTrue(lsn1.equals(lsn3));

            // Consistent hashCode
            assertEquals(lsn1.hashCode(), lsn2.hashCode());
            assertEquals(lsn2.hashCode(), lsn3.hashCode());
        }

        @Test
        @DisplayName("Should handle boundary values in all operations")
        void testBoundaryValues() {
            PostgresLsn maxLong = PostgresLsn.valueOf(Long.MAX_VALUE);
            PostgresLsn minLong = PostgresLsn.valueOf(Long.MIN_VALUE);
            PostgresLsn zero = PostgresLsn.valueOf(0);

            assertNotNull(maxLong.add(zero));
            assertNotNull(minLong.add(zero));
            assertNotNull(maxLong.subtract(zero));
            assertNotNull(minLong.subtract(zero));

            assertTrue(maxLong.compareTo(minLong) > 0);
            assertTrue(zero.compareTo(minLong) > 0);
            assertTrue(zero.compareTo(maxLong) < 0);
        }
    }
}
