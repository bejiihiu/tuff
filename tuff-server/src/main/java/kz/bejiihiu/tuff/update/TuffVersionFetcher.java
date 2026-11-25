package kz.bejiihiu.tuff.update;

import com.destroystokyo.paper.util.VersionFetcher;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import io.papermc.paper.ServerBuildInfo;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.event.ClickEvent;
import net.kyori.adventure.text.format.NamedTextColor;
import org.jspecify.annotations.NullMarked;
import org.jspecify.annotations.Nullable;

import static net.kyori.adventure.text.Component.text;

/**
 * Update checker for Tuff.
 * <p>
 * Tuff releases live on GitHub, so we ask the GitHub API for the newest release tag and compare it
 * with the build we are running. PaperMC's fill/jenkins APIs are not used at all.
 */
@NullMarked
public final class TuffVersionFetcher implements VersionFetcher {

    private static final String REPOSITORY = "bejiihiu/tuff";
    private static final String RELEASES_PAGE = "https://github.com/" + REPOSITORY + "/releases/latest";
    private static final String API_URL = "https://api.github.com/repos/" + REPOSITORY + "/releases/latest";
    private static final String USER_AGENT = "Tuff/"
        + ServerBuildInfo.buildInfo().asString(ServerBuildInfo.StringRepresentation.VERSION_SIMPLE);
    private static final Gson GSON = new Gson();

    @Override
    public long getCacheTime() {
        return 600_000L; // 10 minutes is plenty, no need to hammer the api
    }

    @Override
    public Component getVersionMessage() {
        final ServerBuildInfo info = ServerBuildInfo.buildInfo();
        if (info.buildNumber().isEmpty() && info.gitCommit().isEmpty()) {
            // local dev build, nothing sensible to compare against
            return text("You are running a development build of Tuff", NamedTextColor.YELLOW);
        }

        final @Nullable Release release = this.fetchLatestRelease();
        if (release == null) {
            return text("Could not check for Tuff updates", NamedTextColor.YELLOW);
        }

        final String expectedTag = info.minecraftVersionId() + ".build." + info.buildNumber().orElse(0);
        if (release.tag().equals(expectedTag) || release.tag().equals("v" + expectedTag)) {
            return text("You are running the latest Tuff build", NamedTextColor.GREEN);
        }

        return text("A newer Tuff build is available: " + release.tag(), NamedTextColor.YELLOW)
            .append(Component.newline())
            .append(text("Download: ", NamedTextColor.GRAY)
                .append(text(RELEASES_PAGE, NamedTextColor.GOLD)
                    .hoverEvent(text("Click to open", NamedTextColor.WHITE))
                    .clickEvent(ClickEvent.openUrl(RELEASES_PAGE))));
    }

    private @Nullable Release fetchLatestRelease() {
        try {
            final HttpURLConnection connection = (HttpURLConnection) URI.create(API_URL).toURL().openConnection();
            connection.setConnectTimeout(5000);
            connection.setReadTimeout(5000);
            connection.setRequestProperty("User-Agent", USER_AGENT);
            connection.setRequestProperty("Accept", "application/vnd.github+json");
            if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
                return null;
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream(), StandardCharsets.UTF_8))) {
                final JsonObject json = GSON.fromJson(reader, JsonObject.class);
                final String tag = json.get("tag_name").getAsString();
                return new Release(tag);
            }
        } catch (IOException | RuntimeException _) {
            // network is down, repo is private, github changed the api, whatever - just stay quiet
            return null;
        }
    }

    private record Release(String tag) {
    }
}
