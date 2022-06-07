import unittest
import slurmpy


class TestSlurmJobWrap(unittest.TestCase):
    def test_wrap(self):
        sj = slurmpy.SlurmJob()
        sj.command = 'echo Hello World'
        sj.wrap_slurm_command()
        self.assertIsNotNone(sj.jobid)

    def test_wrap_with_args(self):
        sj = slurmpy.SlurmJob(command='echo Hello World', jobname='testwrap', account='lcni',
                              partition='interactive', output_directory='slurm_out')
        sj.wrap_slurm_command()
        self.assertIsNotNone(sj.jobid)

    def test_wrap_with_bad_args(self):
        sj = slurmpy.SlurmJob(command='echo Hello World', foo='bar')
        sj.wrap_slurm_command()
        self.assertIsNone(sj.jobid)


class TestSlurmJobFile(unittest.TestCase):
    def test_write(self):
        sj = slurmpy.SlurmJob()
        sj.command = 'echo Hello World'
        sj.jobname = 'helloworld'
        filename = sj.write_slurm_file()
        sj.print_slurm_file()
        self.assertIsNotNone(filename)

    def test_submit(self):
        sj = slurmpy.SlurmJob()
        sj.command = 'echo Hello World'
        sj.jobname = 'helloworld'
        sj.write_slurm_file()
        sj.print_slurm_file()
        sj.submit_slurm_file()
        self.assertIsNotNone(sj.jobid)

    def test_array(self):
        sj = slurmpy.SlurmJob()
        sj.command = 'echo Hello $x'
        sj.jobname = 'helloarray'
        sj.array = ['Eugene', 'Springfield', 'Oregon', 'USA', 'World']
        sj.write_slurm_file()
        sj.print_slurm_file()
        sj.submit_slurm_file()
        self.assertIsNotNone(sj.jobid)

    def test_array_with_args(self):
        sj = slurmpy.SlurmJob()
        sj.command = 'echo Hello $x'
        sj.jobname = 'helloarray'
        sj.array = ['Eugene', 'Springfield', 'Oregon', 'USA', 'World']
        sj.output_directory = 'slurm_out'
        sj.partition = 'short'
        sj.write_slurm_file()
        sj.print_slurm_file()
        sj.submit_slurm_file()
        self.assertIsNotNone(sj.jobid)

    # test without submitting a new job; temporary
    def test_output(self):
        sj = slurmpy.SlurmJob(jobname='helloworld', jobid='20627824')
        files = sj.get_output_files()
        print(files)
        self.assertTrue(files)
        sj.show_output()

    # test without submitting a new job; temporary
    def test_output_array(self):
        sj = slurmpy.SlurmJob(jobname='helloarray', jobid='20628519')
        files = sj.get_output_files()
        print(files)
        self.assertTrue(files)
        sj.show_output()

    # test without submitting a new job; temporary
    def test_output_dir(self):
        sj = slurmpy.SlurmJob(jobname='testwrap', jobid='20775614')
        sj.output_directory = 'slurm_out'
        files = sj.get_output_files()
        print(files)
        self.assertTrue(files)
        sj.show_output()

    # test without submitting a new job; temporary
    def test_output_array_dir(self):
        sj = slurmpy.SlurmJob(jobname='helloarray', jobid='20775995')
        sj.output_directory = 'slurm_out'
        files = sj.get_output_files()
        print(files)
        self.assertTrue(files)
        sj.show_output()


if __name__ == '__main__':
    unittest.main()
