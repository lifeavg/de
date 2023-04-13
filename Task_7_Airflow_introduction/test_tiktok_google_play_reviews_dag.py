from tiktok_google_play_reviews_dag import tiktok_google_play_reviews


def test_task_order_correct():
    dag = tiktok_google_play_reviews()

    file_sensor = dag.get_task("wait_tiktok_google_play_reviews_csv")
    extract = dag.get_task("extract")
    filter_content = dag.get_task("transform.filter_content")
    sort_by_creation_date = dag.get_task("transform.sort_by_creation_date")
    fill_nulls = dag.get_task("transform.fill_nulls")
    load = dag.get_task("load")

    assert file_sensor.downstream_list == [extract]
    assert extract.downstream_list == [filter_content]
    assert filter_content.downstream_list == [sort_by_creation_date]
    assert sort_by_creation_date.downstream_list == [fill_nulls]
    assert fill_nulls.downstream_list == [load]
    assert load.downstream_list == []
