def _step_1():
    import logging
    logging.basicConfig(level=logging.INFO)


def _step_2():
    import mlflow
    print(mlflow.version.VERSION)


def test_mlflow():
    assert "aria" != "random_cat"

    _step_1()
    _step_2()

    zero = 2-2
    assert zero == 0
