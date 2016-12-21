INSERT OVERWRITE TABLE profile_interest
SELECT
  p.id,
  p.type,
  p.created,
  p.active,
  i.title
FROM profile p
INNER JOIN interest i ON i.profileId = p.id;
