from types import SimpleNamespace

import backend.main as main


class FakeChronyService:
    def __init__(self, *, available, synchronized, quorum_met):
        self._status = SimpleNamespace(
            available=available,
            synchronized=synchronized,
            quorum_met=quorum_met,
        )

    def snapshot(self):
        return self._status


def test_chrony_nts_gate_requires_availability_synchronization_and_quorum():
    assert main._chrony_nts_quorum_verified(
        FakeChronyService(
            available=True,
            synchronized=True,
            quorum_met=True,
        )
    ) is True


def test_chrony_nts_gate_rejects_unavailable_chrony():
    assert main._chrony_nts_quorum_verified(
        FakeChronyService(
            available=False,
            synchronized=True,
            quorum_met=True,
        )
    ) is False


def test_chrony_nts_gate_rejects_unsynchronized_chrony():
    assert main._chrony_nts_quorum_verified(
        FakeChronyService(
            available=True,
            synchronized=False,
            quorum_met=True,
        )
    ) is False


def test_chrony_nts_gate_rejects_missing_authenticated_quorum():
    assert main._chrony_nts_quorum_verified(
        FakeChronyService(
            available=True,
            synchronized=True,
            quorum_met=False,
        )
    ) is False
