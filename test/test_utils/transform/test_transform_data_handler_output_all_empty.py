from src.transform.transform_utils.transform_data_handler import (
    PandaTransformation,
)
from src.transform.transform_utils.ingestion_s3_handler import (
    IngestionS3Handler,
)


def test_dataframes_dictionary_handles_empty_lists(mocker):
    """
    Test if function returns empty dictionary if all dataframes are empty.
    """
    mocker.patch.object(
        IngestionS3Handler, "get_data_from_ingestion", return_value={}
    )
    test_variable = PandaTransformation()
    result = test_variable.returns_dictionary_of_dataframes()
    expected = {}
    assert result == expected
