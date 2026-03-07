import zq_multiuser as zm


def test_get_bet_status_text_shows_armed_waiting_state():
    rt = {
        "manual_pause": False,
        "switch": True,
        "bet": False,
        "bet_on": False,
        "mode_stop": True,
        "stop_count": 0,
        "pause_countdown_active": False,
    }

    assert zm.get_bet_status_text(rt) == "待机中"
