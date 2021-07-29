//
//  Recommender.swift
//  Recommender
//
//  Created by Tanmay Bakshi on 2021-07-23.
//

import CoreML

actor Recommender {
    static var shared = Recommender()
    
    private var model: MovieLensRecommender!
    
    private init() {
        let config = MLModelConfiguration()
        self.model = try! MovieLensRecommender(configuration: config)
    }
    
    func recommendations(for ratings: [Int64: Double]) throws -> [Int64] {
        let prediction = try model.prediction(input: MovieLensRecommenderInput(items: ratings, k: 30,
                                                                               restrict_: nil, exclude: nil))
        return prediction.recommendations
    }
}
