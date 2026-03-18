# import library
import sys
sys.path.insert(0, 'scripts')

def test_files_dict_not_empty():

    from load_bronze import FILES
    
    # total files should be 7
    assert len(FILES) == 7

def test_files_dict_has_bronze_schema():

    from load_bronze import FILES
    
    # for loop
    for table_name in FILES.keys():
        
        # all table name should be have a prefix bronze
        assert table_name.startswith("bronze."), \
            f"{table_name} must have a prefix bronze."

def test_raw_path_defined():

    from load_bronze import RAW_PATH
    
    # should be have raw path in docker
    assert RAW_PATH is not None
    
    # should be have a length
    assert len(RAW_PATH) > 0