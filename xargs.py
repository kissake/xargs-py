#!/usr/bin/python3

'''
An implementation of xargs functionality for python.
'''

import subprocess
import logging as logging



tab = '\t'
newline = '\n'

##################################
##################################
#
# FUNCTIONS
#
##################################
##################################


def executePipeline(pipelineOfCommandsList, index, outputAppend):
    # By default, do NOT consume the input of the parent.  It isn't insane to have
    # multiple processes pulling from STDIN of the parent process; it just doesn't
    # often make sense.
    nextInput = subprocess.DEVNULL 
    lastOutput = None # By default, inherit parent's STDOUT.
    processList = []

    pipeline = [ commandList[index] for commandList in pipelineOfCommandsList ]
    logging.info("Executing: %s", pipeline)
    
    if len(pipeline[0]) == 1: # Input file
        inputFile = pipeline.pop(0)[0]
        if inputFile is not None:
            nextInput = open(inputFile,'rb')
            logging.debug("Opened %s for reading", inputFile)
        else:
            nextInput = None # Redirect STDIN of parent to the pipeline.
            logging.debug("Using STDIN as input for pipeline")
            
    if len(pipeline[-1]) == 1: # Output file.
        outputFile = pipeline.pop(-1)[0]
        if outputFile is not None:
            if outputAppend:
                lastOutput = open(outputFile, 'a')
                logging.debug("Opened %s for appending", outputFile)
            else:
                lastOutput = open(outputFile, 'w')
                logging.debug("Opened %s for overwriting", outputFile)

        
    while len(pipeline) > 1:
        logging.debug("Starting process %s", pipeline[0])
        process = subprocess.Popen(pipeline.pop(0), stdout = subprocess.PIPE, stdin=nextInput)
        # We don't want to keep the redirected I/O stream open; leave that to the subprocess.
        nextInput.close()

        # Set up the output of this process as the input for the next.
        nextInput = process.stdout

        processList.append(process)

    # This is the last process, and will be redirected to the output file instead:
    logging.debug("Starting process %s", pipeline[0])
    process = subprocess.Popen(pipeline.pop(0), stdout = lastOutput, stdin = nextInput)

    if lastOutput is not None:
        # Remove our open handle to the file, if we had one.
        lastOutput.close()
    
    processList.append(process)
    
    logging.debug("Process list for this pipeline: %s", processList)
    return processList


def waitForPipelineSize(processListList, targetSize):
    logging.debug("There are %s pipelines running, waiting for %s to finish and drop down to %s.", len(processListList), len(processListList)-targetSize, targetSize)
    # Until enough processes have completed to get us to our target size...
    while len(processListList) > targetSize:
        # Cycle through the pipelines in progress...
        for pipelineIndex in range(len(processListList)):
            # And the processes within them....
            for processIndex in reversed(range(len(processListList[pipelineIndex]))):
                # Check to see if the process has completed
                result = processListList[pipelineIndex][processIndex].poll()
                # Two valid results are None (process didn't end) or 0 (process terminated successfully).
                if result == 0:
                    # Remove this process from the list of pending processes in this
                    # pipeline.:
                    processListList[pipelineIndex].pop(processIndex)
                    logging.debug("Completed pipeline %s process %s", pipelineIndex, processIndex)
                elif result is not None: # Non-zero exit codes are errors.
                    raise RuntimeError("Process exited with error code %s"%(result))

        # Remove any pipelines that were emptied.
        # Process the pipelines in reverse order so that we don't change the indexing
        # when we remove empties.
        for pipelineIndex in reversed(range(len(processListList))):
            if len(processListList[pipelineIndex]) == 0:
                # If all of the processes in this pipeline are complete, remove the
                # entry in the list so that we can refill.
                processListList.pop(pipelineIndex)
                logging.debug("Pipeline %s empty", pipelineIndex)
            
        

def xargs(pipelineOfCommandsList, concurrentPipelines, outputAppend=False):

    # A list of lists of processes?
    inProcessList = []
    nextPipeline = 0

    # Assume that all CommandsLists in the pipeline are the same length;
    # this is an error otherwise.  Also assumes we have access to the entire
    # input list; it is possible that we could implement with iterators later.
    while nextPipeline < len(pipelineOfCommandsList[0]):

        # Wait for enough processes to complete to bring the number of running
        # pipelines down to one less than the max.
        waitForPipelineSize(inProcessList, concurrentPipelines - 1)


        while len(inProcessList) < concurrentPipelines and nextPipeline < len(pipelineOfCommandsList[0]):
            logging.debug("Executing new pipeline number %s", nextPipeline)
            inProcessList.append(executePipeline(pipelineOfCommandsList, nextPipeline, outputAppend))
            nextPipeline = nextPipeline + 1

    # Now we have put all of our commands into the pipeline, we just have to wait for them to finish.
    waitForPipelineSize(inProcessList, 0)


##################################
##################################
#
# MAIN
#
##################################
##################################

    

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s:%(message)s', level=logging.DEBUG)

    xargs([
        [
            ['infile1'],   ['infile2'],
        ],
        [
            ['grep','CCACTACTT',],      ['grep','CCACTACTT',],
        ],
        [
            ['fold','-w', '10',],       ['fold','-w', '10',], 
        ],
        [
            ['outfile1',], ['outfile2',],
        ],
    ], 1, outputAppend=False)
    
