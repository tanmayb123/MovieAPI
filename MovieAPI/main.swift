//
//  main.swift
//  MovieAPI
//
//  Created by Tanmay Bakshi on 2021-07-19.
//

import Foundation
import Vapor

actor Sessions {
    static var shared = Sessions()

    private var sessions: [String: MovieContent] = [:]

    private init() {}

    func session(with id: String) -> MovieContent? {
        return sessions[id]
    }

    func addSession(with id: String, content: MovieContent) {
        sessions[id] = content
    }
}

struct MovieSocketRequest: Codable {
    var query: String
    var nextPage: Bool
}

struct MovieSocketResponse: Codable {
    var jobComplete: Bool
    var results: [Movie]
    var error: String?
}

struct GenresResponse: Codable {
    var genres: [String]
    var error: String?
}

func registerVaporEndpoints(application: Application) {
    application.getAsync("register") { req async throws -> String in
        let handler: Db2Handler
        do {
            let authSettings = try req.query.decode(Db2Handler.AuthSettings.self)
            handler = try Db2Handler(authSettings: authSettings)
        } catch let error {
            return "Couldn't register. Error: \(error)"
        }
        let movieContent = MovieContent(db2Handler: handler)
        let identifier = UUID().uuidString
        await Sessions.shared.addSession(with: identifier, content: movieContent)
        return identifier
    }
    
    application.webSocketAsync("movie") { req, ws async in
        guard let sessionId = req.query[String.self, at: "session"] else {
            let response = MovieSocketResponse(jobComplete: false, results: [], error: "No session ID!")
            ws.send(String(data: try! JSONEncoder().encode(response), encoding: .utf8)!)
            return
        }
        guard let session = await Sessions.shared.session(with: sessionId) else {
            let response = MovieSocketResponse(jobComplete: false, results: [], error: "Invalid session ID!")
            ws.send(String(data: try! JSONEncoder().encode(response), encoding: .utf8)!)
            _ = ws.close()
            return
        }
        var search: MovieContent.Search?
        ws.onTextAsync { ws, text async in
            let response = await { () async -> MovieSocketResponse in
                do {
                    let request = try JSONDecoder().decode(MovieSocketRequest.self, from: text.data(using: .utf8)!)
                    let searchTask: MovieContent.Search
                    if request.nextPage {
                        if search == nil {
                            return MovieSocketResponse(jobComplete: false, results: [], error: "Trying to get next page without first page request!")
                        }
                        searchTask = search!
                        guard searchTask.query == request.query else {
                            return MovieSocketResponse(jobComplete: false, results: [], error: "Trying to get next page with different previous request!")
                        }
                    } else {
                        searchTask = try await session.movies(by: request.query)
                        search = searchTask
                    }
                    guard let results = try await searchTask.next() else {
                        search = nil
                        return MovieSocketResponse(jobComplete: true, results: [], error: nil)
                    }
                    return MovieSocketResponse(jobComplete: false, results: results, error: nil)
                } catch let error {
                    return MovieSocketResponse(jobComplete: false, results: [], error: "Error thrown: \(error)")
                }
            }()
            ws.send(String(data: try! JSONEncoder().encode(response), encoding: .utf8)!)
        }
    }
    
    application.getAsync("genres", ":movieID") { req async throws -> String in
        let response = try await { () async throws -> GenresResponse in
            guard let movieIDString = req.parameters.get("movieID") else {
                return GenresResponse(genres: [], error: "No movie ID given")
            }
            guard let movieID = Int(movieIDString) else {
                return GenresResponse(genres: [], error: "Invalid movie ID")
            }
            guard let sessionId = req.query[String.self, at: "session"] else {
                return GenresResponse(genres: [], error: "No session ID")
            }
            guard let session = await Sessions.shared.session(with: sessionId) else {
                return GenresResponse(genres: [], error: "Invalid session ID")
            }
            
            guard let movie = try await session.movie(by: movieID) else {
                return GenresResponse(genres: [], error: "Non-existent movie ID")
            }
            guard let genres = try await session.genres(for: movie) else {
                return GenresResponse(genres: [], error: "Invalid response from Db2")
            }
            
            return GenresResponse(genres: genres, error: nil)
        }()
        
        return String(data: try JSONEncoder().encode(response), encoding: .utf8)!
    }
}

var env = try Environment.detect()
try LoggingSystem.bootstrap(from: &env)
let app = Application(env)

let corsConfiguration = CORSMiddleware.Configuration(
    allowedOrigin: .all,
    allowedMethods: [.POST, .GET, .PATCH, .PUT, .DELETE, .OPTIONS],
    allowedHeaders: []
)
let corsMiddleware = CORSMiddleware(configuration: corsConfiguration)
app.middleware.use(corsMiddleware)

defer { app.shutdown() }
registerVaporEndpoints(application: app)
try app.run()