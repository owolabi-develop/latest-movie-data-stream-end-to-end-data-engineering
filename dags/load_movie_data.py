import requests
from pprint import pprint
import json



def get_video_url(video_id):
    url = f"https://api.themoviedb.org/3/movie/{video_id}/videos?language=en-US"

    headers = {
        "accept": "application/json",
        "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIwYmI5NTE3ZWY3YTgwOTU1MzFhYjVhZGU3YzkzMmU3NSIsInN1YiI6IjYyNTAzMmMxYjZjMjY0MTA1OGJmMjg3NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.DEqtb_abwcVJf25GENMt7GJykqi8pXkZRbEk3lWLnUk"
    }

    video_response_url = requests.get(url, headers=headers).json()['results']

    videourls = [f"https://www.youtube.com/watch?v={url['key']}" for url in video_response_url]
    
    return videourls





def get_trending_movies():
        import requests
        url = "https://api.themoviedb.org/3/trending/movie/day?language=en-US"

        headers = {
            "accept": "application/json",
            "Authorization": "Bearer eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiIwYmI5NTE3ZWY3YTgwOTU1MzFhYjVhZGU3YzkzMmU3NSIsInN1YiI6IjYyNTAzMmMxYjZjMjY0MTA1OGJmMjg3NCIsInNjb3BlcyI6WyJhcGlfcmVhZCJdLCJ2ZXJzaW9uIjoxfQ.DEqtb_abwcVJf25GENMt7GJykqi8pXkZRbEk3lWLnUk"
        }

        movie_response = requests.get(url, headers=headers).json()['results']
        movies = [dict({"adult":movie['adult'],
                    "backdrop_path":movie['backdrop_path'],
                    "genre_ids":movie['genre_ids'],
                    "id":movie['id'],
                    "media_type":movie['media_type'],
                    "original_language":movie['original_language'],
                    "original_title":movie['original_title'],
                    "overview":movie['overview'],
                    "popularity":movie['popularity'],
                    "poster_path":movie['poster_path'],
                    "release_date":movie['release_date'],
                    "title":movie['title'],
                    "video":get_video_url(movie['id']),
                    "vote_average":movie['vote_average'],
                    "vote_count":movie['vote_count']}) for movie in movie_response]
        return movies











