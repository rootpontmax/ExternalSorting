#include "Utils.h"

#include <fstream>
#include <iostream>
#include <cassert>
#include <memory>

////////////////////////////////////////////////////////////////////////////////////////////////////
uint64_t GetProcessTime()
{
    rusage ru;
    if( getrusage( RUSAGE_SELF, &ru ) != -1 )
    {
        const uint64_t ms = ru.ru_utime.tv_sec * 1000 + ru.ru_utime.tv_usec / 1000;
        return ms;
    }
    return 0;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
void CreateHuge( const char *pFilename )
{
    const size_t fileSizeCount = 1 << 30;
    const size_t BUFFER_SIZE = 1 << 18;
    const size_t chunkSize = sizeof( int ) * BUFFER_SIZE;
    const size_t chunkCount = fileSizeCount / BUFFER_SIZE;
    assert( 0 == fileSizeCount % BUFFER_SIZE );
    std::cout << "    Create file with " << fileSizeCount << " integers" << std::endl;
    std::fstream file( pFilename, std::ios_base::out | std::ios_base::binary );
    
    const uint64_t timeBefore = GetProcessTime();
    
    for( size_t i = 0; i < chunkCount; ++i )
    {
        int buffer[BUFFER_SIZE];
        for( int j = 0; j < BUFFER_SIZE; ++j )
            buffer[j] = rand();
        
        file.write( (const char *)&buffer[0], chunkSize );
    }
    
    file.close();

    const uint64_t timeAfter = GetProcessTime();
    const int timeLapse = static_cast< int >( timeAfter - timeBefore );
    std::cout << "    Unsorted file was created for " << timeLapse << " ms" << std::endl;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
bool CheckHuge( const char *pFilename, const size_t memorySize )
{
    std::fstream file( pFilename, std::ios_base::in | std::ios_base::binary );
    if( !file.is_open() )
    {
        std::cout << "Can't open output file with name: " << pFilename << std::endl;
        return false;
    }
    
    // Define size of file
    file.seekg( 0, file.end );
    const size_t fileSize = file.tellg();
    file.seekg( 0, file.beg );
    
    const size_t intCount = memorySize / sizeof( int );
    std::unique_ptr< int[] > pData( new int[intCount] );
    char *pRawData = reinterpret_cast< char* >( pData.get() );
    
    // Define chunk count
    const size_t chunkCount = ( fileSize > memorySize ) ? ( fileSize / memorySize ) : 1;
    for( size_t chunkID = 0; chunkID < chunkCount; ++chunkID )
    {
        file.read( pRawData, memorySize );
        const size_t readedSize = file.gcount();
        const int readedCount = static_cast< int >( readedSize / sizeof( int ) );
        for( int i = 0; i < ( readedCount - 1 ); ++i )
            if( pData[i] > pData[i + 1] )
                return false;
    }
    
    return true;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
size_t GetFileSize( std::fstream& file )
{
    const size_t cachedPos = file.tellg();
    
    file.seekg( 0, file.end );
    const size_t fileSize = file.tellg();
    file.seekg( cachedPos, file.beg );
    
    return fileSize;
}
////////////////////////////////////////////////////////////////////////////////////////////////////
