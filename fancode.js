async function callApisInParallel() {
  try {
    // Define the two API endpoints
    const PRIMARY =
      "https://raw.githubusercontent.com/drmlive/fancode-live-events/refs/heads/main/fancode.json";
    const SECONDARY =
      "https://raw.githubusercontent.com/Jitendraunatti/fancode/refs/heads/main/data/fancode.json";

    // Call both APIs in parallel using Promise.allSettled
    const results = await Promise.allSettled([
      fetch(PRIMARY).then((res) => res.json()),
      fetch(SECONDARY).then((res) => res.json()),
    ]);

    // Process results, only including fulfilled responses
    const successfulResults = results
      .filter((result) => result.status === "fulfilled")
      .map((result) => result.value);

    // If any API call failed, return empty array
    if (results.some((result) => result.status === "rejected")) {
      console.warn("One or more API calls failed, returning empty array");
      return [];
    }
    const matches = [];
    const response1 = await successfulResults[0].matches;
    const response2 = await successfulResults[1].matches
    response1?.forEach((match) => {
      if (match?.adfree_url && match.status === "LIVE") {
        matches.push({
          title: match.match_name,
          image: match.src,
          link: match.adfree_url,
          match_id: match.match_id,
        });
      }
    });
    response2?.forEach((match) => {
      if (match?.STREAMING_CDN?.dai_google_cdn && match.status === "STARTED") {
        matches.push({
          title: match.title,
          image: match.image,
          link: match.STREAMING_CDN.dai_google_cdn,
          match_id: match.match_id,
        });
      }
    });
    const output = matches.reduce((acc, current) => {
      if (!acc.some((item) => item.match_id === current.match_id)) {
        acc.push(current);
      }
      return acc;
    }, []);
    console.log(JSON.stringify(output));
    return output;
  } catch (error) {
    console.error("Unexpected error calling APIs:", error);
    return [];
  }
}

callApisInParallel();
