////////////////////////////////////////////////////////////////////////////////////////////////////
//
//  main.cpp
//  Sorter
//
//  Created by Mikhail Scherbakov on 30/08/16.
//  Copyright © 2016 valve. All rights reserved.
//
////////////////////////////////////////////////////////////////////////////////////////////////////
#include <iostream>
#include <thread>

#include "Utils.h"
#include "Sorter.h"

////////////////////////////////////////////////////////////////////////////////////////////////////
static const size_t g_memorySize = 256 << 20;
static const char  *g_pInputFilename = "HugeInput.txt";
static const char  *g_pOutputFilename = "HugeOutput.txt";
////////////////////////////////////////////////////////////////////////////////////////////////////
int main(int argc, const char * argv[])
{
    const int coreNumber = std::thread::hardware_concurrency();
    std::cout << "Sorter for " << coreNumber << " core(s) and " << g_memorySize << " byte(s)"<< std::endl;

    //CreateHuge( g_pInputFilename );
    
    CSorter sorter( coreNumber, g_memorySize );
    
    const uint64_t timeSortBefore = GetProcessTime();
    sorter.Sort( g_pInputFilename, g_pOutputFilename );
    const uint64_t timeSortAfter = GetProcessTime();
    const int timeLapse = static_cast< int >( timeSortAfter - timeSortBefore );
    std::cout << "    File was sorted for " << timeLapse << " ms" << std::endl;
    
    std::cout << "Sorting checking" << std::endl;
    const bool res = CheckHuge( g_pOutputFilename, g_memorySize );
    
    if( res )
        std::cout << "File is sorted" << std::endl;
    else
        std::cout << "File is unsorted" << std::endl;
    
    return 0;
}
////////////////////////////////////////////////////////////////////////////////////////////////////