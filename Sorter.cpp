#include "Sorter.h"
#include "Utils.h"


#include <thread>
#include <mutex>
#include <iostream>
#include <cassert>

////////////////////////////////////////////////////////////////////////////////////////////////////
SMergeThread::SMergeThread() :
    bHasSecondFile( false ),
    bWasStarted( false )
{}
////////////////////////////////////////////////////////////////////////////////////////////////////
CSorter::SMerge::SMerge() :
    pos( 0 ),
    count( 0 ),
    bWasFinished( false ),
    bReadyToFinish( false )
{}
////////////////////////////////////////////////////////////////////////////////////////////////////
CSorter::CSorter( const int coreCount, const size_t availiableMemory ) :
    m_coreCount( coreCount ),
    m_availableMemory( availiableMemory )
{
    if( coreCount <= 0 )
        throw std::domain_error( "Wrong core count" );
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool CSorter::Sort( const char *pInputFilename, const char *pOutputFilename )
{
    // Multithread partition
    const uint64_t timeA = GetProcessTime();
    if( !PartialSort( pInputFilename ) )
        return false;
    
    const uint64_t timeB = GetProcessTime();
    const int timeAB = static_cast< int >( timeB - timeA );
    std::cout << "    Partial sort for " << timeAB << " ms" << std::endl;

    // One thread merge
    Merge( pOutputFilename );
    const uint64_t timeC = GetProcessTime();
    const int timeBC = static_cast< int >( timeC - timeB );
    std::cout << "    Merge for " << timeBC << " ms" << std::endl;
    
    return true;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
static void ThreadFunctionSort( const int threadID, const int elementInChunk, TSortVec& vec, std::mutex& mtx )
{
    // Allocate array for sorting
    std::vector< int > array( elementInChunk );
    array.resize( elementInChunk );
    char *pData = reinterpret_cast< char* >( &array[0] );
    
    for( ; ; )
    {
        // Find first unsorted chunk and sort it
        int workID = -1;
        mtx.lock();
        for( int i = 0; i < vec.size(); ++i )
            if( !vec[i].bWasStarted )
            {
                vec[i].bWasStarted = true;
                workID = i;
                break;
            }
        mtx.unlock();
    
        // No more unsorted chunks
        if( -1 == workID )
            return;
        
        SSortThread& sortThread = vec[workID];
        
        // Load chunk, sort and store
        std::fstream file( sortThread.pInputFileName, std::ios_base::in | std::ios_base::binary );
        if( !file.is_open() )
        {
            std::cout << "Can't open file: " << sortThread.pInputFileName << std::endl;
            continue;
        }
        
        // Define file size
        file.seekg( 0, file.end );
        const size_t fileSize = file.tellg();
        file.seekg( 0, file.beg );
        
        const size_t fileStart = sortThread.startPos * sizeof( int );
        const size_t chunkSize = sortThread.count * sizeof( int );
        
        // Check chunk boundaries
        if( fileStart >= fileSize )
        {
            std::cout << "Start position is incorrect for file: " << sortThread.pInputFileName << std::endl;
            continue;
        }
        
        // Jump to start position and load
        file.seekg( fileStart, file.beg );
        file.read( pData, chunkSize );
        const size_t readedSize = file.gcount();
        const int readedCount = static_cast< int >( readedSize / sizeof( int ) );
        file.close();

        // Sort
        std::sort( array.begin(), array.begin() + readedCount );
        
        // Store as new file
        std::fstream oFile( sortThread.outputFilename.c_str(), std::ios_base::out | std::ios_base::binary );
        oFile.write( (const char *)&array[0], readedSize );
        oFile.close();
        
        std::cout << "    ID " << threadID << " " << sortThread.outputFilename.c_str() << " " << readedCount << std::endl;
        
        sortThread.realCount = readedCount;
        sortThread.bWasSorted = true;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////
static void CheckAndFillup( std::fstream& file, TVec *pVec, size_t *pCount, size_t *pPos, bool *pIsReadyForFinish, bool *pWasFinished )
{
    if( *pCount > 0 )
        return;
    
    if( true == *pIsReadyForFinish )
    {
        *pWasFinished = true;
        return;
    }
    else
    {
        // Read new portion of data to merge
        const size_t countToRead = pVec->capacity();
        const size_t sizeToRead = countToRead * sizeof( int );
        
        // Load new portion of data and define
        char *pData = reinterpret_cast< char* >( &pVec->operator[](0) );
        file.read( pData, sizeToRead );
        
        // Define count of readed element(s)
        const size_t readedSize = file.gcount();
        const size_t readedCount = readedSize / sizeof( int );
        
        assert( readedCount <= countToRead );
        
        if( readedSize < sizeToRead )
            *pIsReadyForFinish = true;
        if( readedCount == 0 )
            *pWasFinished = true;

        *pCount = readedCount;
        *pPos = 0;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////
static void CheckAndStore( std::fstream& file, TVec *pVec, size_t *pCount,
                           const size_t capacity, const bool bWasFinishedA, const bool bWasFinishedB )
{
    if( *pCount >= capacity || ( !bWasFinishedA && !bWasFinishedB ) )
    {
        const size_t sizeToWrite = ( *pCount ) * sizeof( int );
        char *pData = reinterpret_cast< char* >( &pVec->operator[](0) );\
        file.write( pData, sizeToWrite );
        *pCount = 0;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////
static void ThreadFunctionMerge( const int threadID, const size_t countInp, const size_t countOut, TMergeVec& vec, std::mutex& mtx )
{
    // Allocate input arraies
    std::vector< int > arrayA( countInp );
    std::vector< int > arrayB( countInp );
    arrayA.resize( countInp );
    arrayB.resize( countInp );
    
    // Allocate merge array
    std::vector< int > arrayMerge( countOut );
    arrayMerge.resize( countOut );
    
    // Find the job in infinite loop
    for( ; ; )
    {
        // Find first unsorted chunk and merge it
        int workID = -1;
        mtx.lock();
        for( int i = 0; i < vec.size(); ++i )
            if( !vec[i].bWasStarted )
            {
                vec[i].bWasStarted = true;
                workID = i;
                break;
            }
        mtx.unlock();
        
        // No more unsmerged chunks
        if( -1 == workID )
            return;
        
        // Out working data
        SMergeThread& data = vec[workID];
        
        if( !data.bHasSecondFile )
            continue;
        
        // Open files
        std::fstream fileA( data.pFilenameA, std::ios_base::in | std::ios_base::binary );
        std::fstream fileB( data.pFilenameB, std::ios_base::in | std::ios_base::binary );
        std::fstream fileM( data.mergedFilename, std::ios_base::out | std::ios_base::binary );
        
        // File sizes and its element counts
        const size_t fileSizeM = GetFileSize( fileM );
        const size_t fileCountM = fileSizeM / sizeof( int );
        
        // Merge data and flags
        size_t countA = 0;
        size_t countB = 0;
        size_t countM = 0;
        size_t posA = 0;
        size_t posB = 0;
        bool bWasFinishedA = false;
        bool bWasFinishedB = false;
        bool bIsReadyForFinishA = false;
        bool bIsReadyForFinishB = false;
        
        // Merging
        while( !bWasFinishedA || !bWasFinishedB )
        {
            // Read or save
            CheckAndFillup( fileA, &arrayA, &countA, &posA, &bIsReadyForFinishA, &bWasFinishedA );
            CheckAndFillup( fileB, &arrayB, &countB, &posB, &bIsReadyForFinishB, &bWasFinishedB );
            CheckAndStore( fileM, &arrayMerge, &countM, fileCountM, bWasFinishedA, bWasFinishedB );
            
            // Merge
            if( !bWasFinishedA && !bWasFinishedB )
            {
                // Compare if both arrays are valid
                if( arrayA[posA] < arrayB[posB] )
                {
                    arrayMerge[countM] = arrayA[posA];
                    ++posA;
                    --countA;
                }
                else
                {
                    arrayMerge[countM] = arrayB[posB];
                    ++posB;
                    --countB;
                }
                ++countM;
            }
            else if( !bWasFinishedA && bWasFinishedB )
            {
                // arrayA is valid and arrayB is invalid. So just copy from A to M
                arrayMerge[countM] = arrayA[posA];
                ++posA;
                --countA;
                ++countM;
            }
            else if( bWasFinishedA && !bWasFinishedB )
            {
                // arrayA is invalid and arrayB is valid. So just copy from B to M
                arrayMerge[countM] = arrayB[posB];
                ++posB;
                --countB;
                ++countM;
            }
            
        }
        
        // Delete two input files
        const int resDelA = remove( data.pFilenameA );
        const int resDelB = remove( data.pFilenameB );
        if( resDelA != 0 )
            std::cout << "Error during deleting fileA " << data.pFilenameA << std::endl;
        if( resDelB != 0 )
            std::cout << "Error during deleting fileB " << data.pFilenameB << std::endl;
        
        std::cout << "    ID " << threadID << " finished merge into: " << data.mergedFilename.c_str() << std::endl;
    }
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool CSorter::PartialSort( const char *pFilename )
{
    // Open file and define its size
    std::fstream file( pFilename, std::ios_base::in | std::ios_base::binary );
    if( !file.is_open() )
    {
        std::cout << "Can't open input file with name: " << pFilename << std::endl;
        return false;
    }
    
    // Define size of file
    file.seekg( 0, file.end );
    const size_t fileSize = file.tellg();
    file.seekg( 0, file.beg );
    
    // Define chunk count and size
    const size_t threadMemory = m_availableMemory / m_coreCount;
    const size_t chunkCount = ( 0 == fileSize % threadMemory) ? ( fileSize / threadMemory ) : ( fileSize / threadMemory ) + 1;
    const size_t chunkSize = fileSize / chunkCount;
    const int elementInChunk = static_cast< int >( chunkSize / sizeof( int ) );
    
    // Create and start bunch of threads
    m_sortThread.reserve( chunkCount );
    int pos = 0;
    for( int i = 0; i < chunkCount; ++i )
    {
        char buffer[20];
        snprintf( buffer, 20, "tmpFile_%04d.txt", i );
        SSortThread sortThred;
        
        sortThred.startPos = pos;
        sortThred.count = elementInChunk;
        sortThred.pInputFileName = pFilename;
        sortThred.outputFilename = buffer;
        sortThred.bWasStarted = false;
        sortThred.bWasSorted = false;
        
        m_sortThread.push_back( sortThred );
        
        pos += elementInChunk;
    }
    file.close();
    
    // Start thread pool
    std::mutex mtx;
    std::vector< std::thread > threadPool;
    threadPool.reserve( m_coreCount );
    for( int i = 0; i < m_coreCount; ++i )
        threadPool.push_back( std::thread( ThreadFunctionSort, i, elementInChunk, std::ref( m_sortThread ), std::ref( mtx ) ) );
    
    // Waiting until all process finished
    for( int i = 0; i < m_coreCount; ++i )
        threadPool[i].join();
        
    return true;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
void CSorter::Merge( const char *pFilename )
{
    // Create new file for sorting results
    std::fstream oFile;
    oFile.open( pFilename, std::ios_base::out | std::ios_base::binary );
    oFile.close();
    
    // Define general constants
    const size_t totalCount = m_availableMemory / sizeof( int );
    
    // Define initial size of mergeVec
    const size_t chunkCount = m_sortThread.size();
    const size_t initMergeMaxSize = ( 0 == chunkCount % 2 ) ? ( chunkCount / 2 ) : ( chunkCount / 2 + 1 );
    TMergeVec mergeVec;
    mergeVec.reserve( initMergeMaxSize );
    
    // Initial fill the input vector with chunk names
    TMergeVec inputMergeVec;
    inputMergeVec.resize( chunkCount );
    for( size_t i = 0; i < chunkCount; ++i )
        inputMergeVec[i].mergedFilename = m_sortThread[i].outputFilename.c_str();
    
    int stage = 0;
    for( ; ; )
    {
        const size_t inputSize = inputMergeVec.size();
        const size_t mergeMaxSize = ( 0 == inputSize % 2 ) ? ( inputSize / 2 ) : ( inputSize / 2 + 1 );
        mergeVec.resize( mergeMaxSize );
        for( size_t i = 0; i < mergeMaxSize; ++i )
        {
            char buffer[30];
            snprintf( buffer, 30, "tmpMergeFile_%04d_%04d.txt", stage, (int)i );
            
            const size_t idA = i * 2;
            const size_t idB = idA + 1;
            mergeVec[i].pFilenameA = inputMergeVec[idA].mergedFilename.c_str();
            if( idB < inputSize )
            {
                mergeVec[i].pFilenameB = inputMergeVec[idB].mergedFilename.c_str();
                mergeVec[i].mergedFilename = buffer;
                mergeVec[i].bHasSecondFile = true;
            }
            else
            {
                mergeVec[i].mergedFilename = mergeVec[i].pFilenameA;
                mergeVec[i].bHasSecondFile = false;
            }
            mergeVec[i].bWasStarted = false;
        }
        
        // If we have just one merge task change its merge name to outputFilename
        if( mergeMaxSize == 1 )
            mergeVec[0].mergedFilename = pFilename;
            
        
        // Defince required thread count and its buffer sizes
        const size_t activeThreadCount = ( mergeMaxSize > m_coreCount ) ? m_coreCount : mergeMaxSize;
        if( 0 == activeThreadCount )
            break;
        const size_t elementInChunk = totalCount / activeThreadCount / 4;
        const size_t countInput = elementInChunk;
        const size_t countOutput = elementInChunk * 2;
        std::cout << "Start merge with " << activeThreadCount << " thread(s)" << std::endl;
        
        // Start thread pool
        std::mutex mtx;
        std::vector< std::thread > threadPool;
        threadPool.reserve( activeThreadCount );
        for( int i = 0; i < activeThreadCount; ++i )
            threadPool.push_back( std::thread( ThreadFunctionMerge, i, countInput, countOutput, std::ref( mergeVec ), std::ref( mtx ) ) );
        
        // Waiting until all process finished
        for( int i = 0; i < activeThreadCount; ++i )
            threadPool[i].join();
        
        std::cout << "Merge finished for stage " << stage << std::endl;
        
        // Refresh input merge data
        inputMergeVec.resize( mergeMaxSize );
        if( mergeMaxSize > 1 )
            for( size_t i = 0; i < mergeMaxSize; ++i )
                inputMergeVec[i].mergedFilename = mergeVec[i].mergedFilename;
        
        if( mergeMaxSize == 1 )
            break;
        
        ++stage;
    }
    
    std::cout << "Merge completed" << std::endl;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
