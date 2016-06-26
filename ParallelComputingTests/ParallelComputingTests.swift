//
//  ParallelComputingTests.swift
//  ParallelComputingTests
//
//  Created by Carlos Rodríguez Domínguez on 25/6/16.
//  Copyright © 2016 Everyware Technologies. All rights reserved.
//

import XCTest
@testable import ParallelComputing

class ParallelComputingTests: XCTestCase {
    private let transform = { (value:Int)->Double in
        usleep(50)
        return sqrt(Double(value))
    }
    
    private let combine = { (current:Int, value:Int)->Int in
        usleep(50)
        return current+value
    }
    
    private lazy var defaultParallelExecutor:ParallelExecutor = {
        let queue = NSOperationQueue()
        queue.maxConcurrentOperationCount = 10
        queue.qualityOfService = .UserInitiated
        
        return queue
    }()
    
    private lazy var defaultPartitionExecutor:ParallelExecutor = {
        let queue = NSOperationQueue()
        queue.maxConcurrentOperationCount = 100
        queue.qualityOfService = .UserInitiated
        
        return queue
    }()
    
    private let range = 1...1_000
    
    override func setUp() {
        super.setUp()
        // Put setup code here. This method is called before the invocation of each test method in the class.
    }
    
    override func tearDown() {
        // Put teardown code here. This method is called after the invocation of each test method in the class.
        super.tearDown()
    }
    
    func testAParallelMapProducesTheSameResultsAsASequentialMap() {
        let firstResult = range.map(transform)
        let secondResult = range.parallelMap(executor: defaultParallelExecutor, transform: transform)
        
        for (idx, value) in firstResult.enumerate() {
            XCTAssertTrue(value == secondResult[idx])
        }
    }
    
    func testAPartitionedParallelMapProducesTheSameResultsAsASequentialMap() {
        let firstResult = range.map(transform)
        let secondResult = range.partitionedParallelMap(executor: defaultPartitionExecutor, numberOfPartitions: 71, transform: transform)
        
        for (idx, value) in firstResult.enumerate() {
            if value != secondResult[idx] {
                print(value)
                print(secondResult[idx])
                print(idx)
            }
            
            XCTAssertTrue(value == secondResult[idx])
        }
    }
    
    func testAParallelReduceProducesTheSameResultsAsASequentialReduce() {
        let firstResult = range.reduce(11, combine: combine)
        let secondResult = range.parallelReduce(executor: defaultParallelExecutor, initial:11, combine: combine)
        
        XCTAssertTrue(firstResult == secondResult)
    }
    
    func testAPartitionedParallelReduceProducesTheSameResultsAsASequentialReduce() {
        let firstResult = range.reduce(11, combine: combine)
        let secondResult = range.partitionedParallelReduce(executor: defaultPartitionExecutor, numberOfPartitions: 71, initial:11, combine: combine)
        
        XCTAssertTrue(firstResult == secondResult)
    }
    
    func testPartitionsAreCorrectlyCalculated() {
        range.partitionedParallelMap(executor: defaultPartitionExecutor, numberOfPartitions: 17, transform: transform)
        range.partitionedParallelMap(executor: defaultPartitionExecutor, numberOfPartitions: 32, transform: transform)
        range.partitionedParallelMap(executor: defaultPartitionExecutor, numberOfPartitions: 0, transform: transform)
        range.partitionedParallelMap(executor: defaultPartitionExecutor, numberOfPartitions: 50_000, transform: transform)
    }
    
    func testPerformanceOfSequentialMap() {
        self.measureBlock {
            let _ = self.range.map(self.transform)
        }
    }
    
    func testPerformanceOfSequentialReduce() {
        self.measureBlock {
            let _ = self.range.reduce(0, combine: self.combine)
        }
    }
    
    func testPerformanceOfParallelMap() {
        self.measureBlock {
            let _ = self.range.parallelMap(executor: self.defaultParallelExecutor, transform: self.transform)
        }
    }
    
    func testPerformanceOfPartitionedParallelMap() {
        self.measureBlock {
            let _ = self.range.partitionedParallelMap(executor: self.defaultPartitionExecutor, numberOfPartitions: 100, transform: self.transform)
        }
    }
    
    func testPerformanceOfParallelReduce() {
        self.measureBlock {
            let _ = self.range.parallelReduce(executor: self.defaultParallelExecutor, initial: 0, combine: self.combine)
        }
    }
    
    func testPerformanceOfPartitionedParallelReduce() {
        self.measureBlock {
            let _ = self.range.partitionedParallelReduce(executor: self.defaultPartitionExecutor,  numberOfPartitions: 100, initial: 0, combine: self.combine)
        }
    }
}
