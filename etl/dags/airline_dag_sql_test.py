import internal_unit_testing


def test_dag_import():
    from . import airline_dag_sql

    internal_unit_testing.assert_has_valid_dag(airline_dag_sql)
