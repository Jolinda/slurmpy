import unittest
from slurmpy import SlurmJob

class TestSlurmJob(unittest.TestCase):
    def test_wrap(self):
        sj = SlurmJob()
        sj.command = 'echo Hello World'
        sj.wrap_slurm_command()
        self.assertIsNotNone(sj.jobid)


if __name__ == '__main__':
    unittest.main()
