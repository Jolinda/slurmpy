"""module to assist in SLURM job submission

This module was developed to assist with SLURM job submission on the 
talapas high performance computing cluser at the University of Oregon.
"""

import glob
import os
import re
import subprocess
import sys

default_format = ['jobid%20', 'jobname%25', 'partition', 'state', 'elapsed',
                  'MaxRss']
"""default format for job display in job_info
"""
slurmpy_params = ['command', 'interpreter', 'threads', 'dependency', 'deptype', 'email',
                  'output_directory', 'variable', 'array', 'array_limit', 'jobid', 'filename']

def submit_slurm_file(filename, **slurm_params):
    """ submit a file to slurm using sbatch

    Submits a file to slurm using sbatch, and prints the stdout from 
    the sbatch command

    Parameters
    ----------
    filename: path to file

    Returns
    -------
    jobid if successful
    None if file not found
    """

    if not os.path.exists(filename):
        print('{} not found'.format(filename))
        return None

    options = ""
    for arg in slurm_params:
        options += '--{}={} '.format(arg, slurm_params[arg])

    # stops working when options included?
    # I no longer know what that comment means

    process = subprocess.run(['sbatch', filename],
                             stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             universal_newlines=True)

    print(process.stdout)

    if process.stdout.split()[0] == 'Submitted':
        jobid = process.stdout.split()[-1]
    else:
        jobid = None

    return jobid


def wrap_slurm_command(command, jobname=None, dependency=None,
                       output_directory=None,
                       email=None, threads=None, deptype='ok',
                       **slurm_params):
    """submit command to slurm using sbatch --wrap

    Parameters
    ----------
    command: str or list[str]
        command or list of commands to submit
    jobname: str, optional
        name to give job, if not included jobname will be 'wrap'
    threads: int or int string
        number of threads to used, identical to --cpus-per-task
    email: str, optional
        email address for --mail-user notification
    dependency: int or string, optional
        defer start of job until dependency compltes
    deptype: str, default = 'ok'
        only used if dependency is set
        how the parent job must end. may be ok, any, burstbuffer,
        notok, corr. See sbatch documentation for more info.
    output_directory: path string, optional
        directory to write {jobname}.out/{jobname}.err files to
        directory will be created if it doesn't exist
    **slurm_params: 
        additional slurm parameters

    Returns
    -------
    slurm jobid if successful, None if not

    Examples
    --------
    >>> wrap_slurm_command('echo hello')

    >>> wrap_slurm_command(['module load fsl', 'fslinfo somefile'],
                         jobname = 'example',
                         email = 'mymail@someaddress.com',
                         account = 'lcni',
                         partition = 'short')

     >>> wrap_slurm_command(['module load fsl', 'fslinfo somefile'],
                         jobname = 'example',
                         **{'partition': 'short',
                            'account': 'lcni',
                            'tasks-per-cpu': 1})    

    You must use the last example's format when defining slurm 
    parameters containing dashes                  
    
    """

    slurm = 'sbatch '

    if jobname:
        slurm += '--job-name={} '.format(jobname)

    if email:
        slurm += '--mail-user={} --mail-type=ALL '.format(email)

    if dependency:
        slurm += '--dependency=after{}:{} '.format(deptype, dependency)

    if threads:
        slurm += '--cpus-per-task={} '.format(threads)

    for arg in slurm_params:
        if slurm_params[arg] and arg not in slurmpy_params:
            slurm += '--{}={} '.format(arg, slurm_params[arg])

    if output_directory:
        if not os.path.exists(output_directory):
            os.mkdir(output_directory)
        slurm += '--output={}/%x-%j.out '.format(output_directory)
        slurm += '--error={}/%x-%j.err '.format(output_directory)

    if type(command) is str:
        command = [command]

    slurm += '--wrap \"' + '\n'.join(command) + '"'

    print(slurm)
    process = subprocess.run(slurm, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                             universal_newlines=True, shell=True)

    print(process.stdout)

    if process.stdout.split()[0] == 'Submitted':
        return process.stdout.split()[-1]
    else:
        return None


def write_slurm_file(jobname, command, filename=None,
                     interpreter='bash',
                     array=None, variable='x',
                     output_directory=None, dependency=None,
                     threads=None, array_limit=None, deptype='ok',
                     email=None, **slurm_params):
    """Write a script to be submitted to slurm using sbatch

    Parameters
    ----------
    jobname: str
        name to give job
    command: str or list[str]
        command or list of commands to run
    filename: str, optional
        name for script file, will be jobname.srun if not given
    interpreter: str, optional
        path to interpreter
        if 'bash' (default), will use /bin/bash
    threads: int or int string, optional
        number of threads to used, identical to --cpus-per-task
    email: str, optional
        email address for --mail-user notification
    dependency: int or string, optional
        defer start of job until dependency compltes
    deptype: str, default = 'ok'
        only used if dependency is set
        how the parent job must end. may be ok, any, burstbuffer,
        notok, corr. See sbatch documentation for more info.
    output_directory: path string, optional
        directory to write {jobname}.out/{jobname}.err files to
        directory will be created if it doesn't exist
    array: list, optional
        array to use for job array
    variable: string, default = 'x'
        variable to use for array substitution in command
    array_limit: int
        maximum number of concurrently running tasks
        NOT CURRENTLY IMPLEMENTED
    **slurm_params
        additional slurm parameters

    Returns
    -------
    filename of slurm script

    Examples
    --------
    >>> write_slurm_file('example', 'echo hello')
    example.srun

    >>> write_slurm_file('fslinfo',
                        ['module load fsl', 'fslinfo somefile'],
                        jobname = 'fslinfo',
                        email = 'mymail@someaddress.com',
                        account = 'lcni',
                        partition = 'short')
    fslinfo.srun

    >>> write_slurm_file('fslinfo',
                        ['module load fsl', 'fslinfo ${x}'],
                        filename = 'fslinfo_array.srun'
                        jobname = 'fslinfo',
                        array = ['file1', 'file2', 'file3'],
                        account = 'lcni')
    fslinfo_array.srun

    >>> write_slurm_file('fslinfo',
                        ['module load fsl', 'fslinfo somefile'],
                        **{'partition': 'short',
                           'account': 'lcni',
                           'tasks-per-cpu': 1})    

    You must use the last example's format when defining slurm 
    parameters containing dashes                  
    
    """

    if not filename:
        filename = jobname + '.srun'

    with open(filename, 'w') as f:
        if interpreter == 'python':
            f.write('#!{}\n'.format(sys.executable))

        elif interpreter == 'bash':
            f.write('#!/bin/bash\n')

        else:  # caller sent full path to interpreter
            f.write('#!{}\n'.format(interpreter))

        f.write('#SBATCH --job-name={}\n'.format(jobname))

        if email:
            f.write('#SBATCH --mail-user={}\n'.format(email))
            f.write('#SBATCH --mail-type=END\n')

        if dependency:
            f.write('#SBATCH --dependency=after{}:{}\n'.format(deptype, dependency))

        if threads:
            f.write('#SBATCH --cpus-per-task={}\n'.format(threads))

        for arg in slurm_params:
            if slurm_params[arg] and arg not in slurmpy_params:
                f.write('#SBATCH --{}={}\n'.format(arg, slurm_params[arg]))

        if output_directory:
            if not os.path.exists(output_directory):
                os.mkdir(output_directory)
            if array:
                f.write('#SBATCH --output={}/%x-%A_%a.out\n'.format(output_directory))
                f.write('#SBATCH --error={}/%x-%A_%a.err\n\n'.format(output_directory))
            else:
                f.write('#SBATCH --output={}/%x-%j.out\n'.format(output_directory))
                f.write('#SBATCH --error={}/%x-%j.err\n\n'.format(output_directory))

        if array:
            f.write('#SBATCH --array=0-{}'.format(len(array) - 1))
            if array_limit:
                f.write('%{}'.format(array_limit))
            f.write('\n\ndata=({})\n\n'.format(' '.join(array)))
            f.write('{}=${{data[$SLURM_ARRAY_TASK_ID]}}\n\n'.format(variable))
            # if variable not in command:
            #   print('Warning: {} not found in {}. Are you sure about this?'.format(variable, command))

        if type(command) is str:
            command = [command]
        f.write('\n')
        f.write('\n'.join(command))

    return filename


def notify(jobid, email, **kwargs):
    """notify by email when an existing job finishes

     This is for when you forget to add notification in the first place.
     It creates a job that depends on the first job and sends a 
     notification when it finishes.

     Parameters
     ---------
     jobid
        jobid for the job you want to be notified about
     email
        where the notification should be sent
     **kwargs
        option keyword arguments for WrapSlurmCommands

     Returns
     -------
     jobid of echo done command

     """
    return (wrap_slurm_command(command='echo done', email=email,
                               dependency=jobid, deptype='any',
                               **kwargs))


def job_status(jobid):
    """return status(es) of job

    Parameter
    ---------
    jobid
        id of job

    Returns
    -------
    list
        state of each job in array

    """

    status = []
    for line in job_info(jobid, ['jobid', 'state'], noheader=True).split('\n'):
        if line.split() and '+' not in line.split()[0]:
            status.append(line.split())
    return status


def job_info(jobid, format_list=None, noheader=None):
    """ returns information about job. Use with print().

    Parameters
    ----------
    jobid
        id of job

    format_list: optional
        format to use with sacct

    noheader: bool, optional
        include header in output

    Returns
    -------
    stdout from !sacct command.
    """

    if format_list is None:
        format_list = default_format
    command = ['sacct', '-j', str(jobid), '--format',
               ','.join(format_list)]
    if noheader:
        command.append('-n')
    process = subprocess.run(command, stdout=subprocess.PIPE,
                             stderr=subprocess.STDOUT,
                             universal_newlines=True)
    return process.stdout


def show_status(jobid):
    """ print status of job (condensed)
    """
    statuses = [x[1] for x in job_status(jobid)]
    for x in set(statuses):
        print(x, statuses.count(x))


class SlurmJob:
    """ class defining a slurm job

    Parameters
    ----------
    jobname: str
        name to give job
    command: str or list[str]
        command or list of commands to run
    filename: str
        name for script file, will be jobname.srun if not given
    interpreter: str
        path to interpreter
        if 'bash' (default), will use /bin/bash
    threads: int or int string
        number of threads to used, identical to --cpus-per-task
    email: str
        email address for --mail-user notification
    dependency: int or string
        defer start of job until dependency compltes
    deptype: str, default = 'ok'
        only used if dependency is set
        how the parent job must end. may be ok, any, burstbuffer,
        notok, corr. See sbatch documentation for more info.
    output_directory: path string
        directory to write {jobname}.out/{jobname}.err files to
        directory will be created if it doesn't exist
    array: list
        array to use for job array
    variable: string, default = 'x'
        variable to use for array substitution in command
    array-limit: int
        maximum number of concurrently running tasks
        NOT CURRENTLY IMPLEMENTED
    Any other parameters will be treated as SBATCH arguments

    Examples
    --------
    sj = SlurmJob(jobname = 'example', account = 'pirg')
    sj.command = 'echo hello'
    sj.wrap_slurm_command()

    sj = SlurmJob()
    sj.time = 5
    sj.command = ['echo ${x}', 'srun script.sh ${x}']
    sj.array = ['file1', 'file2']
    sj.write_slurm_file('example2.srun')
    sj.submit_slurm_file()


    """

    def __init__(self, jobname=None, jobid=None, filename=None, **kwargs):
        self.jobname = jobname
        self.jobid = jobid
        self.filename = filename
        for key in kwargs:
            setattr(self, key, kwargs[key])

    def write_slurm_file(self):

        """Write a script to be submitted to slurm using sbatch

        Returns
        -------
        filename of slurm script

        Raises
        ------
        ValueError if jobname not set

"""
        if not self.jobname:
            raise ValueError('jobname not set')

        if not self.filename:
            self.filename = '{}.srun'.format(self.jobname)

        params = {k: vars(self)[k] for k in vars(self)}
        slurmfile = write_slurm_file(**params)

        return slurmfile

    def submit_slurm_file(self):
        """Submit script to job manager

        Returns
        -------
        jobid of spawned job

        """
        self.jobid = submit_slurm_file(self.filename)
        return self.jobid

    def wrap_slurm_command(self):
        """Submit command to slurm using "wrap"

        Returns
        -------
        jobid of spawned job
        """

        params = {k: vars(self)[k] for k in vars(self)}
        self.jobid = wrap_slurm_command(**params)

        return self.jobid

    # I considered reading the slurm script file to get this, but there
    # just isn't a totally clean satisfying way to do it that looks any
    # better than this way. Just know that this might not work if you 
    # supply a real output file name without a jobid field.
    # In that case you really don't need this.
    # also this isn't working right now

    def get_output_files(self, extension='all'):
        """Get a list of the output files slurm wrote to

        Parameters
        ----------
        extension: str, default = 'all'
            If all, return all output files. Otherwise only
            return files ending in .extension. 

        Returns
        -------
        list of output files
        """
        if not hasattr(self, 'jobid'):
            return list()

        if hasattr(self, 'output_directory') and self.output_directory:
            filename = os.path.join(self.output_directory,
                                    '{}-{}'.format(self.jobname, self.jobid))
        else:
            filename = 'slurm-{}'.format(self.jobid)
        if extension == 'all':
            return sorted(glob.glob(filename + '*.*'))
        else:
            return sorted(glob.glob(filename + '*.' + extension))

    def notify(self, email):
        """ send a notification when job finishes

            You don't need to do this if you defined the email
            parameter before the job was submitted.

            Parameters
            ----------
            email: str
                email address to notify

            Raises
            ------
            AttributeError if email=None


        """
        return notify(jobid=self.jobid, email=email)

    def job_status(self):
        """return status of job
        """
        return job_status(self.jobid)

    def job_info(self, format_list=None, noheader=None):
        """Return printable information about job
        """
        if format_list is None:
            format_list = default_format
        return job_info(self.jobid, format_list, noheader)

    def show_status(self):
        """print summarized status of job
        """
        return show_status(self.jobid)

    def show_output(self, index=0, extension='all'):
        """print contents of output files

        Parameters
        ----------
        index: int, optional, default = 0
            index of job step to print
        extension: str, optional, default = 'all'
            If all, print all output files. Otherwise only
            print files ending in .extension.

        """

        files = self.get_output_files()
        files_to_show = [x for x in files if not re.search('_[^{}]'.format(index), x)]
        for file in files_to_show:
            print(file)
            with open(file, encoding='utf-8') as f:
                print(f.read())

    def print_slurm_file(self):
        """print the slurm script
        """
        with open(self.filename) as f:
            print(f.read())
