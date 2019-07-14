////////////////////////////////////////////////////////////////////////////////////////////////////
#pragma once
#include <cstddef>
#include <vector>
#include <string>
#include <fstream>

////////////////////////////////////////////////////////////////////////////////////////////////////
struct SSortThread
{
    std::string outputFilename;
    int         startPos;
    int         count;
    int         realCount;
    const char *pInputFileName;
    bool        bWasStarted;
    bool        bWasSorted;
};
////////////////////////////////////////////////////////////////////////////////////////////////////
struct SMergeThread
{
    SMergeThread();
    std::string mergedFilename;
    const char *pFilenameA;
    const char *pFilenameB;
    bool        bHasSecondFile;
    bool        bWasStarted;
};
////////////////////////////////////////////////////////////////////////////////////////////////////
typedef std::vector< int >          TVec;
typedef std::vector< SSortThread >  TSortVec;
typedef std::vector< SMergeThread > TMergeVec;
////////////////////////////////////////////////////////////////////////////////////////////////////
class CSorter
{
public:
    CSorter( const int coreCount, const size_t availiableMemory );
    bool Sort( const char *pInputFilename, const char *pOutputFilename );
    
private:
    
    struct SMerge
    {
        SMerge();
        std::vector< int >  data;
        std::fstream        file;
        int                 pos;
        int                 count;
        bool                bWasFinished;
        bool                bReadyToFinish;
    };
    
    bool    PartialSort( const char *pFilename );
    void    Merge( const char *pFilename );
    
    const int       m_coreCount;
    const size_t    m_availableMemory;
    TSortVec        m_sortThread;
};
////////////////////////////////////////////////////////////////////////////////////////////////////
