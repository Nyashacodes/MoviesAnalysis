const { Client } = require('pg');

const connectionString = 'postgres://movie_6fil_user:aRNoDKGN2xadtQ2uZopJWdVzQBDyGX8e@dpg-cm5hiivqd2ns73eoocsg-a.oregon-postgres.render.com/movie_6fil?sslmode=require';

const client = new Client({
  connectionString: connectionString,
});

async function fetchData() {
  try {
    await client.connect();

    const resultMoviesByRating = await client.query(`
      SELECT m.title, AVG(r.rating) AS average_rating
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      GROUP BY m.title
      HAVING COUNT(r.rating) >= 5
      ORDER BY average_rating DESC
      LIMIT 5
    `);

    console.log('Top 5 Movies by Average Rating:');
    console.table(resultMoviesByRating.rows);


    const resultUniqueRaterCount = await client.query(`
      SELECT COUNT(DISTINCT rater_id) AS unique_rater_count
      FROM ratings
    `);

    console.log('Number of Unique Raters:', resultUniqueRaterCount.rows[0].unique_rater_count);

    const resultTop5RaterIDs = await client.query(`
      SELECT rater_id, COUNT(movie_id) AS movies_rated, AVG(rating) AS average_rating
      FROM ratings
      GROUP BY rater_id
      HAVING COUNT(movie_id) >= 5
      ORDER BY movies_rated DESC, average_rating DESC
      LIMIT 5
    `);

    console.log('Top 5 Rater IDs:');
    console.table(resultTop5RaterIDs.rows);

    const resultTopRatedMovies = await client.query(`
      SELECT m.title, AVG(r.rating) AS average_rating
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE m.director = 'Michael Bay'
        OR m.genre = 'Comedy'
        AND m.year = 2013
        AND m.country = 'India'
      GROUP BY m.title
      HAVING COUNT(r.rating) >= 5
      ORDER BY average_rating DESC
      LIMIT 5
    `);

    console.log('Top Rated Movies by Criteria:');
    console.table(resultTopRatedMovies.rows);

    // Favorite Movie Genre of Rater ID 1040
    const resultFavoriteGenreForRaterID = await client.query(`
      SELECT m.genre, COUNT(*) AS count
      FROM movies m
      JOIN ratings r ON m.id = r.movie_id
      WHERE r.rater_id = 1040
      GROUP BY m.genre
      ORDER BY count DESC
      LIMIT 1
    `);

    console.log('Favorite Movie Genre of Rater ID 1040:', resultFavoriteGenreForRaterID.rows[0].genre);
  } catch (error) {
    console.error('Error executing query', error);
  } finally {
    await client.end();
  }
}

fetchData();