SELECT title, count(1) as count
FROM interest
GROUP BY title
ORDER BY title;
