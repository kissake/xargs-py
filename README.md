# xargs.py
Python module to implement xargs-like (but better) functionality.

The POSIX utility xargs is _fantastic_.

However, we can do better, particularly if we are using a programming language that offers more than your average shell... like python.

The included python file implements a library.  This library offers the following features over xargs:
 - Possible to implement a pipeline.
 - You don't need to worry about terminating anything (because it's Python; Python knows where its strings end).
 - You don't need any special data structures; just lists of lists of lists of strings.
 
Is it perfect?  Nope.

Is it pretty darn good?  I think so, yes.

Example usage:

    xargs([
        [
            ['infile1',],                ['infile2',],
        ],
        [
            ['grep','CCACTACTT',],      ['grep','CCACTACTT',],
        ],
        [
            ['fold','-w', '10',],       ['fold','-w', '10',], 
        ],
        [
            ['outfile1',],              ['outfile2',],
        ],
    ], 1)
