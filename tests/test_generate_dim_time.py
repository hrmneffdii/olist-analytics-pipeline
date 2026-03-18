# import library
import sys
import pandas as pd

# setup in docker way
sys.path.insert(0, 'scripts')

from generate_dim_date import generate_dim_time

def test_generate_dim_time_returns_dataframe():
    
    # get dim time as dataframe
    result = generate_dim_time()
    
    # result must be in dataframe
    assert isinstance(result, pd.DataFrame)


def test_dim_time_has_required_columns():

    # expected columns
    required_columns = [
        'date_key', 'full_date', 'year',
        'month', 'quarter', 'is_weekend'
    ]
    
    # get dim time as dataframe
    result = generate_dim_time()
    
    # for loop for all expected column
    for col in required_columns:
        
        # expected column should be cointained by result
        assert col in result.columns, f"column '{col}' not found."


def test_weekend_flag_correct():

    saturday = pd.Timestamp('2024-01-06')  
    sunday   = pd.Timestamp('2024-01-07')  
    monday   = pd.Timestamp('2024-01-08')  

    assert saturday.dayofweek >= 5
    assert sunday.dayofweek >= 5
    assert monday.dayofweek < 5