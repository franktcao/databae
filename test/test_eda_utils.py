import numpy as np
import pandas as pd
from eda_utils import filter_rows, flatten_table, join_single_key_tables


class TestFlattenTable:
    @staticmethod
    def test_simple():
        sample = pd.DataFrame({"key": list("aaabb"), "value": list("12312"),})

        expected = pd.DataFrame(
            [("a", ["1", "2", "3"]), ("b", ["1", "2"]),],
            columns=["key", "value"]
        )
        actual = flatten_table(sample, index_cols="key")

        pd.testing.assert_frame_equal(actual, expected)
    
    @staticmethod
    def test_multi_index():
        sample = pd.DataFrame(
            {
                "key_1": list("aaabb"), 
                "key_2": list("cdcdc"), 
                "value": list("12312"),
            }
        )

        expected = pd.DataFrame(
            [
                ("a", "c", ["1", "3"]), 
                ("a", "d", ["2"]), 
                ("b", "c", ["2"]), 
                ("b", "d", ["1"]), 
            ],
            columns=["key_1", "key_2", "value"]
        )

        actual = flatten_table(sample, index_cols=["key_1", "key_2"])

        pd.testing.assert_frame_equal(actual, expected)


class TestJoinSingleKeyTables:
    @staticmethod
    def test_2_tables() -> None:
        # === Arrange
        sample_1 = pd.DataFrame({"key": list("abcde"), "value1": list("12345")})
        sample_2 = pd.DataFrame({"key": list("abcde"), "value2": list("23456")})

        expected  = pd.DataFrame(
            {
                "key": list("abcde"), 
                "value1": list("12345"), 
                "value2": list("23456")
            }
        )

        # === Act (df keys don't mean anything here but are required)
        actual = join_single_key_tables(key_col="key", a=sample_1, b=sample_2)

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)


    @staticmethod
    def test_3_tables() -> None:
        # === Arrange
        sample_1 = pd.DataFrame({"key": list("abcde"), "value1": list("12345")})
        sample_2 = pd.DataFrame({"key": list("abcde"), "value2": list("23456")})
        sample_3 = pd.DataFrame({"key": list("abcde"), "value3": list("34567")})

        expected  = pd.DataFrame(
            {
                "key": list("abcde"), 
                "value1": list("12345"), 
                "value2": list("23456"),
                "value3": list("34567"),
            }
        )

        # === Act (df keys don't mean anything here but are required)
        actual = join_single_key_tables(
            "key", a=sample_1, b=sample_2, c=sample_3
        )

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)

    @staticmethod
    def test_same_cols() -> None:
        # === Arrange
        sample_1 = pd.DataFrame({"key": list("abcde"), "value1": list("12345")})
        sample_2 = pd.DataFrame({"key": list("abcde"), "value1": list("23456")})

        expected  = pd.DataFrame(
            {
                "key": list("abcde"), 
                "value1__table_1": list("12345"), 
                "value1__table_2": list("23456"),
            }
        )

        # === Act
        actual = join_single_key_tables(
            key_col="key", table_1=sample_1, table_2=sample_2
        )

        # === Assert
        pd.testing.assert_frame_equal(actual, expected)


class TestFilterRows:
    @staticmethod
    def test_simple_keep() -> None:
        # === Arrange
        sample = pd.DataFrame({"a": list("abc")})
        expected = pd.DataFrame({"a": list("ac")})
        
        # === Act
        actual = filter_rows(sample, col="a", to_keep=["a", "c"])
       
        # === Assert
        np.testing.assert_array_equal(actual, expected)
    
    @staticmethod
    def test_simple_drop() -> None:
        # === Arrange
        sample = pd.DataFrame({"a": list("abc")})
        expected = pd.DataFrame({"a": list("b")})
        
        # === Act
        actual = filter_rows(sample, col="a", to_drop=["a", "c"])
       
        # === Assert
        np.testing.assert_array_equal(actual, expected)
    
    @staticmethod
    def test_multicols() -> None:
        # === Arrange
        sample = pd.DataFrame({"a": list("abc"), "b": list("123")})
        expected = pd.DataFrame({"a": list("ac"), "b": list("13")})
        
        # === Act
        actual = filter_rows(sample, col="a", to_keep=["a", "c"])
       
        # === Assert
        np.testing.assert_array_equal(actual, expected)