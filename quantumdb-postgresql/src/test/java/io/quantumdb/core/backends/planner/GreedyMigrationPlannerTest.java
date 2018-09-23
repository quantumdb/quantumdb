package io.quantumdb.core.backends.planner;

import static io.quantumdb.core.schema.definitions.Column.Hint.AUTO_INCREMENT;
import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.Column.Hint.NOT_NULL;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bigint;
import static io.quantumdb.core.schema.definitions.PostgresTypes.bool;
import static io.quantumdb.core.schema.definitions.PostgresTypes.chars;
import static io.quantumdb.core.schema.definitions.PostgresTypes.date;
import static io.quantumdb.core.schema.definitions.PostgresTypes.doubles;
import static io.quantumdb.core.schema.definitions.PostgresTypes.floats;
import static io.quantumdb.core.schema.definitions.PostgresTypes.integer;
import static io.quantumdb.core.schema.definitions.PostgresTypes.oid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.text;
import static io.quantumdb.core.schema.definitions.PostgresTypes.timestamp;
import static io.quantumdb.core.schema.definitions.PostgresTypes.uuid;
import static io.quantumdb.core.schema.definitions.PostgresTypes.varchar;
import static io.quantumdb.core.schema.operations.SchemaOperations.addColumn;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import io.quantumdb.core.backends.planner.Operation.Type;
import io.quantumdb.core.planner.PostgresqlMigrationPlanner;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.ForeignKey;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.versioning.Changelog;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.State;
import lombok.extern.slf4j.Slf4j;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

@Slf4j
@RunWith(Parameterized.class)
public class GreedyMigrationPlannerTest {
	@Rule
	public ErrorCollector collector = new ErrorCollector();

	@Parameters(name = "{1}")
	public static Collection<Object[]> data() {
		return Arrays.asList(new Object[][] {
				{ videoStoreCatalog(), addColumn("stores", "opened", date(), NOT_NULL), 11, 7 }, // 9,7
				{ videoStoreCatalog(), addColumn("customers", "cancelled_membership", date()), 11, 7 }, // 4,3
				{ videoStoreCatalog(), addColumn("staff", "contract_terminated", date()), 11, 7 }, // 9,7
				{ videoStoreCatalog(), addColumn("paychecks", "bounced", bool()), 1, 1 },
				{ videoStoreCatalog(), addColumn("payments", "bounced", bool()), 1, 1 },
				{ videoStoreCatalog(), addColumn("films", "release_date", date()), 4, 4 },
				{ videoStoreCatalog(), addColumn("inventory", "acquired", date()), 3, 3 },
				{ videoStoreCatalog(), addColumn("rentals", "returned", date()), 2, 2 },

				{ studentEducationCatalog(), addColumn("users", "birth_date", date()), 10, 6 },
				{ studentEducationCatalog(), addColumn("students", "gpa", doubles()), 10, 6 },
				{ studentEducationCatalog(), addColumn("student_higher_educations", "reputation", doubles()), 10, 6 },
				{ studentEducationCatalog(), addColumn("student_educations", "reputation", doubles()), 10, 6 },
				{ studentEducationCatalog(), addColumn("experiences", "country", varchar(255)), 10, 6 },
				{ studentEducationCatalog(), addColumn("resumes", "completed", bool()), 10, 6 },

				{ simpleMagnet(), addColumn("users", "registered", date()), 12, 8 },

				{ fullMagnet(), addColumn("users", "registered", date()), 70, 66 },
		});
	}

	private static Supplier<Catalog> fullMagnet() {
		return () -> {
			Catalog catalog = new Catalog("fullMagnet");

			Table answeredQuestions = new Table("answered_questions")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("jobseeker_id", bigint()))
					.addColumn(new Column("is_positive", bool(), NOT_NULL))
					.addColumn(new Column("answer", varchar(255), NOT_NULL))
					.addColumn(new Column("answered_at", timestamp(true), "'now()'", NOT_NULL));

			Table applicationAttachments = new Table("application_attachments")
					.addColumn(new Column("application_id", bigint(), NOT_NULL))
					.addColumn(new Column("attachment_id", bigint(), NOT_NULL, IDENTITY));

			Table applications = new Table("applications")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("recruiter_id", bigint(), NOT_NULL))
					.addColumn(new Column("opportunity_id", bigint(), NOT_NULL))
					.addColumn(new Column("message", text()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("accepted", bool()))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table applicationsResponses = new Table("applications__responses")
					.addColumn(new Column("application_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("recruiter_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("message", text()))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table cloudImages = new Table("cloud_images")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("bucket", varchar(255), NOT_NULL))
					.addColumn(new Column("folder", varchar(255)))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("title", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table companies = new Table("companies")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("employees", integer()));

			Table conversationSubscribers = new Table("conversation_subscribers")
					.addColumn(new Column("conversation_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("subscriber_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("starred", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("archived", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_read_message_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table conversations = new Table("conversations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("title", varchar(255), NOT_NULL))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("sender_id", bigint(), NOT_NULL))
					.addColumn(new Column("receiver_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table databasechangelog = new Table("databasechangelog")
					.addColumn(new Column("id", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("author", varchar(255), NOT_NULL))
					.addColumn(new Column("filename", varchar(255), NOT_NULL))
					.addColumn(new Column("dateexecuted", timestamp(true), NOT_NULL))
					.addColumn(new Column("orderexecuted", integer(), NOT_NULL))
					.addColumn(new Column("exectype", varchar(255), NOT_NULL))
					.addColumn(new Column("md5sum", varchar(255)))
					.addColumn(new Column("description", varchar(255)))
					.addColumn(new Column("comments", varchar(255)))
					.addColumn(new Column("tag", varchar(255)))
					.addColumn(new Column("liquibase", varchar(255)));

			Table databasechangeloglock = new Table("databasechangeloglock")
					.addColumn(new Column("id", integer(), NOT_NULL, IDENTITY))
					.addColumn(new Column("locked", bool(), NOT_NULL))
					.addColumn(new Column("lockgranted", timestamp(true)))
					.addColumn(new Column("lockedby", varchar(255)));

			Table deletedUsers = new Table("deleted_users")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("email", varchar(255), NOT_NULL))
					.addColumn(new Column("deleted_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("found_job_via_magnet", bool()))
					.addColumn(new Column("invalid_email", bool(), "'false'", NOT_NULL));

			Table educationNames = new Table("education_names")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table educations = new Table("educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("institution_id", bigint(), NOT_NULL))
					.addColumn(new Column("education_name_id", bigint(), NOT_NULL))
					.addColumn(new Column("level", varchar(255), NOT_NULL))
					.addColumn(new Column("discipline", varchar(255)))
					.addColumn(new Column("sub_discipline", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table emailOptOut = new Table("email_opt_out")
					.addColumn(new Column("email", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table employeesPerCountry = new Table("employees_per_country")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("country", varchar(255), NOT_NULL))
					.addColumn(new Column("number_of_employees", integer(), NOT_NULL))
					.addColumn(new Column("company_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table eventOrganizers = new Table("event_organizers")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT));

			Table experiences = new Table("experiences")
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("hours_per_week_spent", integer()))
					.addColumn(new Column("weeks_spent", integer(), NOT_NULL))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("institute_name", varchar(255)))
					.addColumn(new Column("country", varchar(255)))
					.addColumn(new Column("city", varchar(255)));

			Table extraCurricularExperiences = new Table("extra_curricular_experiences")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("volunteer", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("work_type", varchar(255)))
					.addColumn(new Column("second_work_type", varchar(255)));

			Table featureFlags = new Table("feature_flags")
					.addColumn(new Column("user_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("feature", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table files = new Table("files")
					.addColumn(new Column("sha", varchar(255), NOT_NULL))
					.addColumn(new Column("media_type", varchar(255)))
					.addColumn(new Column("bytes", oid(), NOT_NULL))
					.addColumn(new Column("created_by_user_id", bigint(), NOT_NULL))
					.addColumn(new Column("created", timestamp(true), NOT_NULL))
					.addColumn(new Column("name", varchar(255)))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table institutions = new Table("institutions")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("url", varchar(255)))
					.addColumn(new Column("verified", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("academic", bool(), NOT_NULL))
					.addColumn(new Column("city", varchar(255)))
					.addColumn(new Column("country", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table invites = new Table("invites")
					.addColumn(new Column("id", uuid(), NOT_NULL, IDENTITY))
					.addColumn(new Column("email", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("invited_by", bigint()))
					.addColumn(new Column("viewed_landing_page", timestamp(true)))
					.addColumn(new Column("joined", timestamp(true)))
					.addColumn(new Column("created_account", bigint()))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table jobSeekerEmailPreferences = new Table("job_seeker_email_preferences")
					.addColumn(new Column("user_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("personal_messages", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("application_response", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("network_requests", varchar(255), "''IMMEDIATELY'::character varying'", NOT_NULL))
					.addColumn(new Column("bounce_problems", timestamp(true)))
					.addColumn(new Column("reported_spam", timestamp(true)))
					.addColumn(new Column("onboarding", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("premium_status", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("profile_views", varchar(255), "''DAILY'::character varying'", NOT_NULL))
					.addColumn(new Column("last_profile_view_sent_at", timestamp(true)));

			Table jobseekersSavedOpportunities = new Table("jobseekers__saved_opportunities")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("jobseeker_id", bigint()))
					.addColumn(new Column("opportunity_id", bigint()))
					.addColumn(new Column("saved_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("opportunity_removed_at", timestamp(true)))
					.addColumn(new Column("company_name", varchar(255)))
					.addColumn(new Column("opportunity_name", varchar(255)));

			Table languageSkills = new Table("language_skills")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("level", varchar(255), NOT_NULL))
					.addColumn(new Column("language", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table magnetTransactions = new Table("magnet_transactions")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("date", timestamp(true), NOT_NULL))
					.addColumn(new Column("delta", integer(), NOT_NULL))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("note", varchar(255)))
					.addColumn(new Column("moderator_id", bigint()))
					.addColumn(new Column("store_item_id", bigint()))
					.addColumn(new Column("purchase_date", timestamp(true)))
					.addColumn(new Column("order_date", timestamp(true)))
					.addColumn(new Column("cancelled", bool(), "'false'"))
					.addColumn(new Column("last_modified_user_id", bigint()))
					.addColumn(new Column("last_modified", timestamp(true)))
					.addColumn(new Column("invited_user_id", bigint()));

			Table messages = new Table("messages")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("body", text(), NOT_NULL))
					.addColumn(new Column("sender_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("conversation_id", bigint()))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table networkOpportunities = new Table("network_opportunities")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("opportunity_id", bigint(), NOT_NULL, IDENTITY));

			Table networks = new Table("networks")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("active", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("last_modified_user_id", bigint()))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("created_by_user_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), NOT_NULL))
					.addColumn(new Column("icon", varchar(255), NOT_NULL))
					.addColumn(new Column("description", varchar(255), NOT_NULL))
					.addColumn(new Column("cover_photo_id", bigint()))
					.addColumn(new Column("invite_percentage", integer(), NOT_NULL))
					.addColumn(new Column("education_weight", varchar(255)))
					.addColumn(new Column("experience_weight", varchar(255)))
					.addColumn(new Column("sports_weight", varchar(255)))
					.addColumn(new Column("abroad_weight", varchar(255)))
					.addColumn(new Column("tools_weight", varchar(255)))
					.addColumn(new Column("language_weight", varchar(255)))
					.addColumn(new Column("extra_cur_weight", varchar(255)))
					.addColumn(new Column("internship_weight", varchar(255)))
					.addColumn(new Column("work_exp_weight", varchar(255)))
					.addColumn(new Column("high_school_edu_weight", varchar(255)))
					.addColumn(new Column("bachelor_edu_weight", varchar(255)))
					.addColumn(new Column("master_edu_weight", varchar(255)));

			Table networksRecruiters = new Table("networks__recruiters")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("recruiter_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("update_subscription", varchar(255), "''IMMEDIATELY'::character varying'", NOT_NULL));

			Table newsPosts = new Table("news_posts")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("body", text(), NOT_NULL))
					.addColumn(new Column("poster_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("opportunity_id", bigint()))
					.addColumn(new Column("organization_id", bigint()))
					.addColumn(new Column("youtube_video_id", bigint()))
					.addColumn(new Column("cloud_image_id", bigint()))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table newsPostsNetworks = new Table("news_posts__networks")
					.addColumn(new Column("news_post_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY));

			Table occupations = new Table("occupations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("started", date(), NOT_NULL))
					.addColumn(new Column("finished", date()))
					.addColumn(new Column("abroad", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("remarks", text()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table opportunities = new Table("opportunities")
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("recruiter_id", bigint(), NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("description", text()))
					.addColumn(new Column("active", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("archived", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("job_starts", date()))
					.addColumn(new Column("response_deadline", timestamp(true)))
					.addColumn(new Column("location", varchar(255)))
					.addColumn(new Column("salary", integer()))
					.addColumn(new Column("currency", varchar(255)))
					.addColumn(new Column("salary_negotiable", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("external_url", varchar(255)))
					.addColumn(new Column("handle_externally", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_modified_user_id", bigint()))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("created_by_user_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), NOT_NULL))
					.addColumn(new Column("level", varchar(255), NOT_NULL))
					.addColumn(new Column("length", integer()))
					.addColumn(new Column("length_negotiable", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("youtube_id", varchar(255)))
					.addColumn(new Column("banner_image_id", bigint()))
					.addColumn(new Column("begin_time", timestamp(true)))
					.addColumn(new Column("end_time", timestamp(true)));

			Table organizationPages = new Table("organization_pages")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("heading", varchar(255), NOT_NULL))
					.addColumn(new Column("text", text()))
					.addColumn(new Column("image_id", bigint()))
					.addColumn(new Column("ordering", integer(), "'0'", NOT_NULL))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("youtube_id", varchar(255)))
					.addColumn(new Column("twitter_handle", varchar(255)))
					.addColumn(new Column("role", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table organizationTags = new Table("organization_tags")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("organization_id", bigint()))
					.addColumn(new Column("color", chars(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("last_used", timestamp(true), "'now()'", NOT_NULL));

			Table organizations = new Table("organizations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("active", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("name", varchar(255)))
					.addColumn(new Column("phone_number", varchar(255)))
					.addColumn(new Column("post_office_box_number", integer()))
					.addColumn(new Column("post_office_box_postal_code", varchar(255)))
					.addColumn(new Column("website", varchar(255)))
					.addColumn(new Column("fax", varchar(255)))
					.addColumn(new Column("email", varchar(255)))
					.addColumn(new Column("facebook_url", varchar(255)))
					.addColumn(new Column("linked_in_url", varchar(255)))
					.addColumn(new Column("twitter_url", varchar(255)))
					.addColumn(new Column("avatar_filename", varchar(255)))
					.addColumn(new Column("industry", varchar(255)))
					.addColumn(new Column("street_name", varchar(255)))
					.addColumn(new Column("house_number", varchar(255)))
					.addColumn(new Column("postal_code", varchar(255)))
					.addColumn(new Column("city", varchar(255)))
					.addColumn(new Column("country", varchar(255)))
					.addColumn(new Column("banner_image_id", bigint()))
					.addColumn(new Column("show_on_home_page", bool(), NOT_NULL))
					.addColumn(new Column("subscription", varchar(255), NOT_NULL))
					.addColumn(new Column("slug", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("pipedrive_id", bigint()))
					.addColumn(new Column("questionnaire_priority", integer()));

			Table pepCountries = new Table("pep_countries")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("country", varchar(255), NOT_NULL));

			Table pepExperienceIndustries = new Table("pep_experience_industries")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("experience_id", bigint(), NOT_NULL))
					.addColumn(new Column("industry", varchar(255), NOT_NULL))
					.addColumn(new Column("weight", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepExperienceWorkTypes = new Table("pep_experience_work_types")
					.addColumn(new Column("experience_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("work_type", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("weight", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterAbroad = new Table("pep_filter_abroad")
					.addColumn(new Column("experience_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("type", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("weight", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducations = new Table("pep_filter_educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("weight", varchar(255), NOT_NULL))
					.addColumn(new Column("reject_policy", varchar(255), NOT_NULL))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("minimum_gpa", floats()))
					.addColumn(new Column("gpa_matters", bool(), NOT_NULL))
					.addColumn(new Column("level", varchar(255)))
					.addColumn(new Column("pace_matters", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("months_before_graduation", integer()))
					.addColumn(new Column("education_id", bigint()))
					.addColumn(new Column("max_months_remaining", integer()))
					.addColumn(new Column("second_study_bonus", bool(), "'false'"))
					.addColumn(new Column("min_months_remaining", integer()))
					.addColumn(new Column("max_delay", integer()))
					.addColumn(new Column("degree_type", varchar(255), "''ALL'::character varying'", NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducationsHasSelectedDisciplines = new Table("pep_filter_educations__has_selected_disciplines")
					.addColumn(new Column("pep_filter_education_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("selected_discipline", varchar(255)))
					.addColumn(new Column("weight", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducationsHasSelectedSubdisciplines = new Table("pep_filter_educations__has_selected_subdisciplines")
					.addColumn(new Column("pep_filter_education_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("selected_subdiscipline", varchar(255), NOT_NULL))
					.addColumn(new Column("weight", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducationsSelectedEducationNames = new Table("pep_filter_educations__selected_education_names")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("pep_filter_education_id", bigint()))
					.addColumn(new Column("included", bool(), NOT_NULL))
					.addColumn(new Column("education_name_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducationsSelectedEducations = new Table("pep_filter_educations__selected_educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("pep_filter_education_id", bigint()))
					.addColumn(new Column("included", bool(), NOT_NULL))
					.addColumn(new Column("education_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterEducationsSelectedInstitutions = new Table("pep_filter_educations__selected_institutions")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("pep_filter_education_id", bigint()))
					.addColumn(new Column("included", bool(), NOT_NULL))
					.addColumn(new Column("institution_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterExperiences = new Table("pep_filter_experiences")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("weight", varchar(255), NOT_NULL))
					.addColumn(new Column("reject_policy", varchar(255), NOT_NULL))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("minimum_weeks_spent", integer()))
					.addColumn(new Column("role", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterLanguages = new Table("pep_filter_languages")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("weight", varchar(255), NOT_NULL))
					.addColumn(new Column("reject_policy", varchar(255), NOT_NULL))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("language", varchar(255), NOT_NULL))
					.addColumn(new Column("minimum_level", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterTools = new Table("pep_filter_tools")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("weight", varchar(255), NOT_NULL))
					.addColumn(new Column("reject_policy", varchar(255), NOT_NULL))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("minimum_level", varchar(255), NOT_NULL))
					.addColumn(new Column("tool_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepFilterTravels = new Table("pep_filter_travels")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("minimum_months", integer(), NOT_NULL))
					.addColumn(new Column("weight", varchar(255), NOT_NULL))
					.addColumn(new Column("reject_policy", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepJobLevels = new Table("pep_job_levels")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("job_level", varchar(255), NOT_NULL, IDENTITY));

			Table pepJobtypes = new Table("pep_jobtypes")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("jobtype", varchar(255), NOT_NULL, IDENTITY));

			Table pepStudyPhases = new Table("pep_study_phases")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("study_phase", varchar(255), NOT_NULL, IDENTITY));

			Table pepUpdateRequests = new Table("pep_update_requests")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("type", varchar(255), NOT_NULL))
					.addColumn(new Column("work_started_at", timestamp(true)))
					.addColumn(new Column("work_started_by", varchar(255)))
					.addColumn(new Column("student_id", bigint()))
					.addColumn(new Column("network_id", bigint()))
					.addColumn(new Column("finished", timestamp(true)))
					.addColumn(new Column("error_id", varchar(255)))
					.addColumn(new Column("requested_by", bigint()))
					.addColumn(new Column("added", integer()))
					.addColumn(new Column("deleted", integer()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table pepWorkTypes = new Table("pep_work_types")
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("work_type", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table premiumRenewals = new Table("premium_renewals")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("start_date", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("invite_id", uuid()))
					.addColumn(new Column("days", integer(), NOT_NULL));

			Table profileViews = new Table("profile_views")
					.addColumn(new Column("id", bigint(), "'nextval('profile_views_id_seq1'::regclass)'", NOT_NULL, IDENTITY))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("recruiter_id", bigint()))
					.addColumn(new Column("date", date(), "'now()'", NOT_NULL))
					.addColumn(new Column("views", integer(), "'1'", NOT_NULL))
					.addColumn(new Column("seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("mailed", bool(), "'false'", NOT_NULL));

			Table purchases = new Table("purchases")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("key", varchar(255), NOT_NULL))
					.addColumn(new Column("permission_value", integer()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table queuedMailTags = new Table("queued_mail_tags")
					.addColumn(new Column("mail_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("tag", varchar(255)));

			Table queuedMails = new Table("queued_mails")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("delivery_time", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("to_name", varchar(255)))
					.addColumn(new Column("to_address", varchar(255), NOT_NULL))
					.addColumn(new Column("subject", varchar(255), NOT_NULL))
					.addColumn(new Column("text_body", text()))
					.addColumn(new Column("inlines", text()))
					.addColumn(new Column("campaign", varchar(255)))
					.addColumn(new Column("from_name", varchar(255)))
					.addColumn(new Column("from_address", varchar(255)))
					.addColumn(new Column("html_body", text()))
					.addColumn(new Column("claimed_by", uuid()))
					.addColumn(new Column("updated_at", timestamp(true)))
					.addColumn(new Column("track_id", varchar(255)));

			Table recruiterEmailPreferences = new Table("recruiter_email_preferences")
					.addColumn(new Column("user_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("personal_messages", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("application", varchar(255), "''IMMEDIATELY'::character varying'", NOT_NULL))
					.addColumn(new Column("bounce_problems", timestamp(true)))
					.addColumn(new Column("reported_spam", timestamp(true)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table recruiters = new Table("recruiters")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("role", text()))
					.addColumn(new Column("visibility", varchar(255), "''NETWORK'::character varying'", NOT_NULL))
					.addColumn(new Column("timeline_feed_intro_seen", bool(), "'false'"));

			Table scheduledNetworkRequestEmails = new Table("scheduled_network_request_emails")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("scheduled", timestamp(true), NOT_NULL))
					.addColumn(new Column("deliver_at", timestamp(true), NOT_NULL))
					.addColumn(new Column("sent_at", timestamp(true)))
					.addColumn(new Column("requests_sent", integer()));

			Table scheduledNewInNetworkEmails = new Table("scheduled_new_in_network_emails")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("recruiter_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("scheduled", timestamp(true), NOT_NULL))
					.addColumn(new Column("deliver_at", timestamp(true), NOT_NULL))
					.addColumn(new Column("sent_at", timestamp(true)))
					.addColumn(new Column("job_seekers_sent", integer()));

			Table scheduledOnboardingEmails = new Table("scheduled_onboarding_emails")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("jobseeker_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("scheduled", timestamp(true), NOT_NULL))
					.addColumn(new Column("deliver_at", timestamp(true), NOT_NULL))
					.addColumn(new Column("sent_at", timestamp(true)));

			Table scheduledQuestions = new Table("scheduled_questions")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("jobseeker_id", bigint(), NOT_NULL))
					.addColumn(new Column("organization_id", bigint(), NOT_NULL))
					.addColumn(new Column("saved_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("is_positive", bool(), NOT_NULL));

			Table sportExperiences = new Table("sport_experiences")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY));

			Table studentCompanySizeInterests = new Table("student_company_size_interests")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("company_size", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table studentHighSchools = new Table("student_high_schools")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("level", varchar(255)))
					.addColumn(new Column("school_name", varchar(255)))
					.addColumn(new Column("city", varchar(255), NOT_NULL))
					.addColumn(new Column("country", varchar(255), NOT_NULL));

			Table studentHigherEducations = new Table("student_higher_educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("aborted", bool(), "'false'"))
					.addColumn(new Column("education_id", bigint()))
					.addColumn(new Column("nominal_duration", integer(), NOT_NULL));

			Table studentInterestedIndustries = new Table("student_interested_industries")
					.addColumn(new Column("student_id", bigint(), IDENTITY))
					.addColumn(new Column("industries", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table studentInterestedWorkTypes = new Table("student_interested_work_types")
					.addColumn(new Column("student_id", bigint(), IDENTITY))
					.addColumn(new Column("types_of_work", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table studentOtherEducations = new Table("student_other_educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("institute", varchar(255)))
					.addColumn(new Column("migrate_suggestion", varchar(255)))
					.addColumn(new Column("city", varchar(255)))
					.addColumn(new Column("country", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table students = new Table("students")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("date_of_birth", date()))
					.addColumn(new Column("invited_by", bigint()))
					.addColumn(new Column("status", varchar(255)))
					.addColumn(new Column("magnets", integer(), "'0'", NOT_NULL))
					.addColumn(new Column("start_date", date()))
					.addColumn(new Column("start_asap", bool(), NOT_NULL))
					.addColumn(new Column("occupied", bool(), NOT_NULL))
					.addColumn(new Column("available", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("interests", varchar(255)))
					.addColumn(new Column("completed_wizard", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("network_page_intro_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("news_feed_intro_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("profile_org_pref_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("profile_cv_completion_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("profile_summary_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("main_experience_id", bigint()))
					.addColumn(new Column("main_education_id", bigint()))
					.addColumn(new Column("professional", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("deactivated", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("profile_views_introduction_seen", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_questionnaire", timestamp(true)));

			Table studentsEducations = new Table("students__educations")
					.addColumn(new Column("gpa", floats()))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("graduated", bool()));

			Table studentsNetworks = new Table("students__networks")
					.addColumn(new Column("student_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("network_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("status", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("updated_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("calculated", bool(), "'true'", NOT_NULL))
					.addColumn(new Column("pep_calculation_date", timestamp(true), "'now()'"))
					.addColumn(new Column("match_score", floats()))
					.addColumn(new Column("responded_at", timestamp(true)));

			Table studentsOrganizationsTags = new Table("students__organizations__tags")
					.addColumn(new Column("student_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("tag_id", bigint()));

			Table studentsPhotos = new Table("students__photos")
					.addColumn(new Column("student_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("cloud_image_id", bigint(), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table studentsUploads = new Table("students__uploads")
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("attachment_id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table subscriptionsPermissions = new Table("subscriptions__permissions")
					.addColumn(new Column("subscription", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("key", varchar(255), NOT_NULL, IDENTITY))
					.addColumn(new Column("permission_value", integer()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table toolSkills = new Table("tool_skills")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("level", varchar(255), NOT_NULL))
					.addColumn(new Column("tool_id", bigint()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table tools = new Table("tools")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table travelExperiences = new Table("travel_experiences")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("summary", varchar(255), NOT_NULL))
					.addColumn(new Column("location", varchar(255), NOT_NULL))
					.addColumn(new Column("started", date(), NOT_NULL))
					.addColumn(new Column("finished", date()))
					.addColumn(new Column("remarks", text()))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table users = new Table("users")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("email", varchar(255), NOT_NULL))
					.addColumn(new Column("first_name", varchar(255), NOT_NULL))
					.addColumn(new Column("last_name", varchar(255), NOT_NULL))
					.addColumn(new Column("avatar_filename", varchar(255)))
					.addColumn(new Column("gender", varchar(255)))
					.addColumn(new Column("nationality", varchar(255)))
					.addColumn(new Column("phone_number", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'", NOT_NULL))
					.addColumn(new Column("last_login", timestamp(true)))
					.addColumn(new Column("login_count", bigint(), "'0'", NOT_NULL))
					.addColumn(new Column("liked_on_facebook", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("newsletter", bool(), "'true'"))
					.addColumn(new Column("last_login_ip", varchar(255)))
					.addColumn(new Column("blocked", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("street_name", varchar(255)))
					.addColumn(new Column("house_number", varchar(255)))
					.addColumn(new Column("postal_code", varchar(255)))
					.addColumn(new Column("city", varchar(255)))
					.addColumn(new Column("country", varchar(255)))
					.addColumn(new Column("fax", varchar(255)))
					.addColumn(new Column("post_office_box_number", integer()))
					.addColumn(new Column("post_office_box_postal_code", varchar(255)))
					.addColumn(new Column("website", varchar(255)))
					.addColumn(new Column("facebook_url", varchar(255)))
					.addColumn(new Column("linked_in_url", varchar(255)))
					.addColumn(new Column("twitter_url", varchar(255)))
					.addColumn(new Column("magneteer", bool(), "'false'"))
					.addColumn(new Column("language", varchar(255)))
					.addColumn(new Column("show_magnet_is_updated", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_seen", timestamp(true)))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table workExperiences = new Table("work_experiences")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("role", varchar(255)))
					.addColumn(new Column("industry", varchar(255), "''OTHER'::character varying'"))
					.addColumn(new Column("internship", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("volunteer", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("work_type", varchar(255)))
					.addColumn(new Column("second_work_type", varchar(255)));

			Table youtubeVideos = new Table("youtube_videos")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY, AUTO_INCREMENT))
					.addColumn(new Column("video_id", varchar(255)))
					.addColumn(new Column("title", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			catalog.addTable(answeredQuestions);
			catalog.addTable(applicationAttachments);
			catalog.addTable(applications);
			catalog.addTable(applicationsResponses);
			catalog.addTable(cloudImages);
			catalog.addTable(companies);
			catalog.addTable(conversationSubscribers);
			catalog.addTable(conversations);
			catalog.addTable(databasechangelog);
			catalog.addTable(databasechangeloglock);
			catalog.addTable(deletedUsers);
			catalog.addTable(educationNames);
			catalog.addTable(educations);
			catalog.addTable(emailOptOut);
			catalog.addTable(employeesPerCountry);
			catalog.addTable(eventOrganizers);
			catalog.addTable(experiences);
			catalog.addTable(extraCurricularExperiences);
			catalog.addTable(featureFlags);
			catalog.addTable(files);
			catalog.addTable(institutions);
			catalog.addTable(invites);
			catalog.addTable(jobSeekerEmailPreferences);
			catalog.addTable(jobseekersSavedOpportunities);
			catalog.addTable(languageSkills);
			catalog.addTable(magnetTransactions);
			catalog.addTable(messages);
			catalog.addTable(networkOpportunities);
			catalog.addTable(networks);
			catalog.addTable(networksRecruiters);
			catalog.addTable(newsPosts);
			catalog.addTable(newsPostsNetworks);
			catalog.addTable(occupations);
			catalog.addTable(opportunities);
			catalog.addTable(organizationPages);
			catalog.addTable(organizationTags);
			catalog.addTable(organizations);
			catalog.addTable(pepCountries);
			catalog.addTable(pepExperienceIndustries);
			catalog.addTable(pepExperienceWorkTypes);
			catalog.addTable(pepFilterAbroad);
			catalog.addTable(pepFilterEducations);
			catalog.addTable(pepFilterEducationsHasSelectedDisciplines);
			catalog.addTable(pepFilterEducationsHasSelectedSubdisciplines);
			catalog.addTable(pepFilterEducationsSelectedEducationNames);
			catalog.addTable(pepFilterEducationsSelectedEducations);
			catalog.addTable(pepFilterEducationsSelectedInstitutions);
			catalog.addTable(pepFilterExperiences);
			catalog.addTable(pepFilterLanguages);
			catalog.addTable(pepFilterTools);
			catalog.addTable(pepFilterTravels);
			catalog.addTable(pepJobLevels);
			catalog.addTable(pepJobtypes);
			catalog.addTable(pepStudyPhases);
			catalog.addTable(pepUpdateRequests);
			catalog.addTable(pepWorkTypes);
			catalog.addTable(premiumRenewals);
			catalog.addTable(profileViews);
			catalog.addTable(purchases);
			catalog.addTable(queuedMailTags);
			catalog.addTable(queuedMails);
			catalog.addTable(recruiterEmailPreferences);
			catalog.addTable(recruiters);
			catalog.addTable(scheduledNetworkRequestEmails);
			catalog.addTable(scheduledNewInNetworkEmails);
			catalog.addTable(scheduledOnboardingEmails);
			catalog.addTable(scheduledQuestions);
			catalog.addTable(sportExperiences);
			catalog.addTable(studentCompanySizeInterests);
			catalog.addTable(studentHighSchools);
			catalog.addTable(studentHigherEducations);
			catalog.addTable(studentInterestedIndustries);
			catalog.addTable(studentInterestedWorkTypes);
			catalog.addTable(studentOtherEducations);
			catalog.addTable(students);
			catalog.addTable(studentsEducations);
			catalog.addTable(studentsNetworks);
			catalog.addTable(studentsOrganizationsTags);
			catalog.addTable(studentsPhotos);
			catalog.addTable(studentsUploads);
			catalog.addTable(subscriptionsPermissions);
			catalog.addTable(toolSkills);
			catalog.addTable(tools);
			catalog.addTable(travelExperiences);
			catalog.addTable(users);
			catalog.addTable(workExperiences);
			catalog.addTable(youtubeVideos);

			answeredQuestions.addForeignKey("organization_id").referencing(organizations, "id");
			answeredQuestions.addForeignKey("jobseeker_id").referencing(students, "id");
			applicationAttachments.addForeignKey("application_id").referencing(applications, "id");
			applicationAttachments.addForeignKey("attachment_id").referencing(files, "id");
			applications.addForeignKey("opportunity_id").referencing(opportunities, "id");
			applications.addForeignKey("recruiter_id").referencing(recruiters, "id");
			applications.addForeignKey("student_id").referencing(students, "id");
			applicationsResponses.addForeignKey("application_id").referencing(applications, "id");
			applicationsResponses.addForeignKey("recruiter_id").referencing(recruiters, "id");
			companies.addForeignKey("id").referencing(organizations, "id");
			conversationSubscribers.addForeignKey("last_read_message_id").referencing(messages, "id");
			conversationSubscribers.addForeignKey("conversation_id").referencing(conversations, "id");
			conversationSubscribers.addForeignKey("subscriber_id").referencing(users, "id");
			conversations.addForeignKey("organization_id").referencing(organizations, "id");
			conversations.addForeignKey("receiver_id").referencing(users, "id");
			conversations.addForeignKey("sender_id").referencing(users, "id");
			educations.addForeignKey("institution_id").referencing(institutions, "id");
			educations.addForeignKey("education_name_id").referencing(educationNames, "id");
			employeesPerCountry.addForeignKey("company_id").referencing(companies, "id");
			eventOrganizers.addForeignKey("id").referencing(organizations, "id");
			experiences.addForeignKey("id").referencing(occupations, "id");
			extraCurricularExperiences.addForeignKey("id").referencing(experiences, "id");
			featureFlags.addForeignKey("user_id").referencing(users, "id");
			files.addForeignKey("created_by_user_id").referencing(users, "id");
			invites.addForeignKey("invited_by").referencing(users, "id");
			invites.addForeignKey("created_account").referencing(users, "id");
			jobSeekerEmailPreferences.addForeignKey("user_id").referencing(students, "id");
			jobseekersSavedOpportunities.addForeignKey("jobseeker_id").referencing(students, "id");
			jobseekersSavedOpportunities.addForeignKey("opportunity_id").referencing(opportunities, "id");
			languageSkills.addForeignKey("student_id").referencing(students, "id");
			magnetTransactions.addForeignKey("invited_user_id").referencing(students, "id");
			magnetTransactions.addForeignKey("moderator_id").referencing(users, "id");
			magnetTransactions.addForeignKey("student_id").referencing(students, "id");
			magnetTransactions.addForeignKey("last_modified_user_id").referencing(users, "id");
			messages.addForeignKey("conversation_id").referencing(conversations, "id");
			messages.addForeignKey("sender_id").referencing(users, "id");
			networkOpportunities.addForeignKey("network_id").referencing(networks, "id");
			networkOpportunities.addForeignKey("opportunity_id").referencing(opportunities, "id");
			networks.addForeignKey("cover_photo_id").referencing(cloudImages, "id");
			networks.addForeignKey("created_by_user_id").referencing(users, "id");
			networks.addForeignKey("organization_id").referencing(organizations, "id");
			networks.addForeignKey("last_modified_user_id").referencing(recruiters, "id");
			networksRecruiters.addForeignKey("network_id").referencing(networks, "id");
			networksRecruiters.addForeignKey("recruiter_id").referencing(recruiters, "id");
			newsPosts.addForeignKey("cloud_image_id").referencing(cloudImages, "id");
			newsPosts.addForeignKey("opportunity_id").referencing(opportunities, "id");
			newsPosts.addForeignKey("organization_id").referencing(organizations, "id");
			newsPosts.addForeignKey("poster_id").referencing(users, "id");
			newsPosts.addForeignKey("youtube_video_id").referencing(youtubeVideos, "id");
			newsPostsNetworks.addForeignKey("network_id").referencing(networks, "id");
			newsPostsNetworks.addForeignKey("news_post_id").referencing(newsPosts, "id");
			occupations.addForeignKey("student_id").referencing(students, "id");
			opportunities.addForeignKey("organization_id").referencing(organizations, "id");
			opportunities.addForeignKey("recruiter_id").referencing(recruiters, "id");
			opportunities.addForeignKey("banner_image_id").referencing(cloudImages, "id");
			opportunities.addForeignKey("created_by_user_id").referencing(recruiters, "id");
			opportunities.addForeignKey("last_modified_user_id").referencing(recruiters, "id");
			organizationPages.addForeignKey("organization_id").referencing(organizations, "id");
			organizationPages.addForeignKey("image_id").referencing(cloudImages, "id");
			organizationTags.addForeignKey("organization_id").referencing(organizations, "id");
			organizationTags.addForeignKey("organization_id").referencing(organizations, "id");
			organizations.addForeignKey("banner_image_id").referencing(cloudImages, "id");
			pepCountries.addForeignKey("network_id").referencing(networks, "id");
			pepExperienceIndustries.addForeignKey("experience_id").referencing(pepFilterExperiences, "id");
			pepExperienceWorkTypes.addForeignKey("experience_id").referencing(pepFilterExperiences, "id");
			pepFilterAbroad.addForeignKey("experience_id").referencing(pepFilterExperiences, "id");
			pepFilterEducations.addForeignKey("network_id").referencing(networks, "id");
			pepFilterEducations.addForeignKey("education_id").referencing(educations, "id");
			pepFilterEducationsHasSelectedDisciplines.addForeignKey("pep_filter_education_id").referencing(pepFilterEducations, "id");
			pepFilterEducationsHasSelectedSubdisciplines.addForeignKey("pep_filter_education_id").referencing(pepFilterEducations, "id");
			pepFilterEducationsSelectedEducationNames.addForeignKey("education_name_id").referencing(educationNames, "id");
			pepFilterEducationsSelectedEducationNames.addForeignKey("pep_filter_education_id").referencing(pepFilterEducations, "id");
			pepFilterEducationsSelectedEducations.addForeignKey("education_id").referencing(educations, "id");
			pepFilterEducationsSelectedEducations.addForeignKey("pep_filter_education_id").referencing(pepFilterEducations, "id");
			pepFilterEducationsSelectedInstitutions.addForeignKey("pep_filter_education_id").referencing(pepFilterEducations, "id");
			pepFilterEducationsSelectedInstitutions.addForeignKey("institution_id").referencing(institutions, "id");
			pepFilterExperiences.addForeignKey("network_id").referencing(networks, "id");
			pepFilterLanguages.addForeignKey("network_id").referencing(networks, "id");
			pepFilterTools.addForeignKey("network_id").referencing(networks, "id");
			pepFilterTools.addForeignKey("tool_id").referencing(tools, "id");
			pepFilterTravels.addForeignKey("network_id").referencing(networks, "id");
			pepJobLevels.addForeignKey("network_id").referencing(networks, "id");
			pepJobtypes.addForeignKey("network_id").referencing(networks, "id");
			pepStudyPhases.addForeignKey("network_id").referencing(networks, "id");
			pepUpdateRequests.addForeignKey("requested_by").referencing(users, "id");
			pepUpdateRequests.addForeignKey("network_id").referencing(networks, "id");
			pepUpdateRequests.addForeignKey("student_id").referencing(students, "id");
			pepWorkTypes.addForeignKey("network_id").referencing(networks, "id");
			premiumRenewals.addForeignKey("invite_id").referencing(invites, "id");
			premiumRenewals.addForeignKey("student_id").referencing(students, "id");
			profileViews.addForeignKey("recruiter_id").referencing(recruiters, "id");
			profileViews.addForeignKey("student_id").referencing(students, "id");
			purchases.addForeignKey("organization_id").referencing(organizations, "id");
			queuedMailTags.addForeignKey("mail_id").referencing(queuedMails, "id");
			recruiterEmailPreferences.addForeignKey("user_id").referencing(recruiters, "id");
			recruiters.addForeignKey("organization_id").referencing(organizations, "id");
			recruiters.addForeignKey("id").referencing(users, "id");
			scheduledNetworkRequestEmails.addForeignKey("student_id").referencing(students, "id");
			scheduledNewInNetworkEmails.addForeignKey("network_id").referencing(networks, "id");
			scheduledNewInNetworkEmails.addForeignKey("recruiter_id").referencing(recruiters, "id");
			scheduledOnboardingEmails.addForeignKey("jobseeker_id").referencing(students, "id");
			scheduledQuestions.addForeignKey("jobseeker_id").referencing(students, "id");
			scheduledQuestions.addForeignKey("organization_id").referencing(organizations, "id");
			sportExperiences.addForeignKey("id").referencing(experiences, "id");
			studentCompanySizeInterests.addForeignKey("student_id").referencing(students, "id");
			studentHighSchools.addForeignKey("id").referencing(studentsEducations, "id");
			studentHigherEducations.addForeignKey("education_id").referencing(educations, "id");
			studentHigherEducations.addForeignKey("id").referencing(studentsEducations, "id");
			studentInterestedIndustries.addForeignKey("student_id").referencing(students, "id");
			studentInterestedWorkTypes.addForeignKey("student_id").referencing(students, "id");
			studentOtherEducations.addForeignKey("id").referencing(studentsEducations, "id");
			students.addForeignKey("main_education_id").referencing(studentHigherEducations, "id");
			students.addForeignKey("main_experience_id").referencing(experiences, "id");
			students.addForeignKey("invited_by").referencing(students, "id");
			students.addForeignKey("id").referencing(users, "id");
			studentsEducations.addForeignKey("id").referencing(occupations, "id");
			studentsNetworks.addForeignKey("student_id").referencing(students, "id");
			studentsNetworks.addForeignKey("network_id").referencing(networks, "id");
			studentsOrganizationsTags.addForeignKey("tag_id").referencing(organizationTags, "id");
			studentsOrganizationsTags.addForeignKey("student_id").referencing(students, "id");
			studentsPhotos.addForeignKey("cloud_image_id").referencing(cloudImages, "id");
			studentsPhotos.addForeignKey("student_id").referencing(students, "id");
			studentsUploads.addForeignKey("attachment_id").referencing(files, "id");
			studentsUploads.addForeignKey("student_id").referencing(students, "id");
			toolSkills.addForeignKey("tool_id").referencing(tools, "id");
			travelExperiences.addForeignKey("student_id").referencing(students, "id");
			workExperiences.addForeignKey("id").referencing(experiences, "id");

			return catalog;
		};
	}

	private static Supplier<Catalog> simpleMagnet() {
		return () -> {
			Catalog catalog = new Catalog("magnet");

			Table experiences = new Table("experiences")
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("hours_per_week_spent", integer()))
					.addColumn(new Column("weeks_spent", integer(), NOT_NULL))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("institute_name", varchar(255)))
					.addColumn(new Column("country", varchar(255)))
					.addColumn(new Column("city", varchar(255)));

			Table occupations = new Table("occupations")
					.addColumn(new Column("id", bigint(), AUTO_INCREMENT, NOT_NULL, IDENTITY))
					.addColumn(new Column("student_id", bigint(), NOT_NULL))
					.addColumn(new Column("started", date(), NOT_NULL))
					.addColumn(new Column("abroad", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table studentHighSchools = new Table("student_high_schools")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("level", varchar(255)))
					.addColumn(new Column("school_name", varchar(255)))
					.addColumn(new Column("city", varchar(255), NOT_NULL))
					.addColumn(new Column("country", varchar(255), NOT_NULL));

			Table studentHigherEducations = new Table("student_higher_educations")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("aborted", bool(), "'false'"))
					.addColumn(new Column("education_id", bigint()))
					.addColumn(new Column("nominal_duration", integer(), NOT_NULL));

			Table studentInterestedIndustries = new Table("student_interested_industries")
					.addColumn(new Column("student_id", bigint(), IDENTITY))
					.addColumn(new Column("industries", varchar(255)))
					.addColumn(new Column("created_at", timestamp(true), "'now()'"))
					.addColumn(new Column("updated_at", timestamp(true)));

			Table students = new Table("students")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("invited_by", bigint()))
					.addColumn(new Column("magnets", integer(), "'0'", NOT_NULL))
					.addColumn(new Column("main_experience_id", bigint()))
					.addColumn(new Column("main_education_id", bigint()))
					.addColumn(new Column("deactivated", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_questionnaire", timestamp(true)));

			Table studentsEducations = new Table("students__educations")
					.addColumn(new Column("gpa", floats()))
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("graduated", bool()));

			Table users = new Table("users")
					.addColumn(new Column("id", bigint(), NOT_NULL, IDENTITY))
					.addColumn(new Column("email", varchar(255), NOT_NULL))
					.addColumn(new Column("first_name", varchar(255), NOT_NULL))
					.addColumn(new Column("last_name", varchar(255), NOT_NULL))
					.addColumn(new Column("blocked", bool(), "'false'", NOT_NULL))
					.addColumn(new Column("last_seen", timestamp(true)))
					.addColumn(new Column("updated_at", timestamp(true)));

			catalog.addTable(experiences);
			catalog.addTable(occupations);
			catalog.addTable(studentHighSchools);
			catalog.addTable(studentHigherEducations);
			catalog.addTable(studentInterestedIndustries);
			catalog.addTable(students);
			catalog.addTable(studentsEducations);
			catalog.addTable(users);

			experiences.addForeignKey("id").referencing(occupations, "id");
			occupations.addForeignKey("student_id").referencing(students, "id");
			studentHighSchools.addForeignKey("id").referencing(studentsEducations, "id");
			studentHigherEducations.addForeignKey("id").referencing(studentsEducations, "id");
			studentInterestedIndustries.addForeignKey("student_id").referencing(students, "id");
			students.addForeignKey("main_education_id").referencing(studentHigherEducations, "id");
			students.addForeignKey("main_experience_id").referencing(experiences, "id");
			students.addForeignKey("invited_by").referencing(students, "id");
			students.addForeignKey("id").referencing(users, "id");
			studentsEducations.addForeignKey("id").referencing(occupations, "id");

			return catalog;
		};
	}

	private static Supplier<Catalog> videoStoreCatalog() {
		return () -> {
			Catalog catalog = new Catalog("test-db");

			Table stores = new Table("stores")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("manager_id", integer(), NOT_NULL));

			Table staff = new Table("staff")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("store_id", integer(), NOT_NULL));

			Table customers = new Table("customers")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL))
					.addColumn(new Column("store_id", integer(), NOT_NULL))
					.addColumn(new Column("referred_by", integer()));

			Table films = new Table("films")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL));

			Table inventory = new Table("inventory")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("store_id", integer(), NOT_NULL))
					.addColumn(new Column("film_id", integer(), NOT_NULL));

			Table paychecks = new Table("paychecks")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("staff_id", integer(), NOT_NULL))
					.addColumn(new Column("date", date(), IDENTITY, NOT_NULL))
					.addColumn(new Column("amount", floats(), NOT_NULL));

			Table payments = new Table("payments")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("staff_id", integer()))
					.addColumn(new Column("customer_id", integer(), NOT_NULL))
					.addColumn(new Column("rental_id", integer(), NOT_NULL))
					.addColumn(new Column("date", date(), IDENTITY, NOT_NULL))
					.addColumn(new Column("amount", floats(), NOT_NULL));

			Table rentals = new Table("rentals")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("staff_id", integer()))
					.addColumn(new Column("customer_id", integer(), NOT_NULL))
					.addColumn(new Column("inventory_id", integer(), NOT_NULL))
					.addColumn(new Column("date", date(), IDENTITY, NOT_NULL));

			stores.addForeignKey("manager_id").referencing(staff, "id");

			staff.addForeignKey("store_id").referencing(stores, "id");

			customers.addForeignKey("referred_by").referencing(customers, "id");
			customers.addForeignKey("store_id").referencing(stores, "id");

			inventory.addForeignKey("store_id").referencing(stores, "id");
			inventory.addForeignKey("film_id").referencing(films, "id");

			paychecks.addForeignKey("staff_id").referencing(staff, "id");

			payments.addForeignKey("staff_id").referencing(staff, "id");
			payments.addForeignKey("customer_id").referencing(customers, "id");
			payments.addForeignKey("rental_id").referencing(rentals, "id");

			rentals.addForeignKey("staff_id").referencing(staff, "id");
			rentals.addForeignKey("customer_id").referencing(customers, "id");
			rentals.addForeignKey("inventory_id").referencing(inventory, "id");

			catalog.addTable(stores);
			catalog.addTable(staff);
			catalog.addTable(customers);
			catalog.addTable(films);
			catalog.addTable(inventory);
			catalog.addTable(payments);
			catalog.addTable(paychecks);
			catalog.addTable(rentals);

			return catalog;
		};
	}

	private static Supplier<Catalog> studentEducationCatalog() {
		return () -> {
			Catalog catalog = new Catalog("test-db");

			Table users = new Table("users")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("name", varchar(255), NOT_NULL));

			Table students = new Table("students")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("referred_by", integer()))
					.addColumn(new Column("main_student_higher_education", integer()))
					.addColumn(new Column("main_experience", integer()));

			Table studentHigherEducations = new Table("student_higher_educations")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("student_education_id", integer(), NOT_NULL));

			Table studentEducations = new Table("student_educations")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("resume_id", integer(), NOT_NULL));

			Table experiences = new Table("experiences")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("resume_id", integer(), NOT_NULL));

			Table resumes = new Table("resumes")
					.addColumn(new Column("id", integer(), IDENTITY, AUTO_INCREMENT, NOT_NULL))
					.addColumn(new Column("student_id", integer(), NOT_NULL));

			students.addForeignKey("id").referencing(users, "id");
			students.addForeignKey("referred_by").referencing(students, "id");
			students.addForeignKey("main_student_higher_education").referencing(studentHigherEducations, "id");
			students.addForeignKey("main_experience").referencing(experiences, "id");

			experiences.addForeignKey("id").referencing(resumes, "id");
			studentHigherEducations.addForeignKey("id").referencing(studentEducations, "id");
			studentEducations.addForeignKey("id").referencing(resumes, "id");
			resumes.addForeignKey("student_id").referencing(students, "id");

			catalog.addTable(users);
			catalog.addTable(students);
			catalog.addTable(experiences);
			catalog.addTable(studentHigherEducations);
			catalog.addTable(studentEducations);
			catalog.addTable(resumes);

			return catalog;
		};
	}

	private final Catalog catalog;
	private final RefLog refLog;
	private final Changelog changelog;
	private final int expectedPlanSize;
	private final int expectedGhostTables;
	private final Plan plan;

	public GreedyMigrationPlannerTest(Supplier<Catalog> scenario, SchemaOperation operation,
			int expectedPlanSize, int expectedGhostTables) {

		this.expectedPlanSize = expectedPlanSize;
		this.expectedGhostTables = expectedGhostTables;

		this.changelog = new Changelog();
		this.catalog = scenario.get();

		this.refLog = RefLog.init(catalog, changelog.getRoot());
		State state = new State(catalog, refLog, changelog);

		changelog.addChangeSet("test", "Michael de Jong", operation);

		PostgresqlMigrationPlanner planner = new PostgresqlMigrationPlanner();
		this.plan = planner.createPlan(state, changelog.getRoot(), changelog.getLastAdded());
		log.info("Constructed migration plan: \n" + plan);
	}

	@Test
	public void testPlanDoesNotExceedExpectedSize() {
		int actualSize = plan.getSteps().size();
		String message = "Calculated plan differs in size to the predicted number of steps: " + expectedPlanSize;
		collector.checkThat(message, actualSize, is(expectedPlanSize));
		collector.checkThat(plan.getGhostTables(), hasSize(expectedGhostTables));
	}

	@Test
	public void testAddNullStepsDoNotDependOnOtherSteps() {
		List<Step> addNullSteps = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.ADD_NULL)
				.collect(Collectors.toList());

		addNullSteps.forEach(step -> {
			String message = "Step: " + step + " has a dependency on another step";
			collector.checkThat(message, step.getDependencies(), empty());
		});
	}

	@Test
	public void testThatAllNullRecordsAreBothAddedAndDropped() {
		Set<Table> addNullRecords = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.ADD_NULL)
				.map(Step::getOperation)
				.flatMap(operation -> operation.getTables().stream())
				.collect(Collectors.toSet());

		Set<Table> dropNullRecords = plan.getSteps().stream()
				.filter(step -> step.getOperation().getType() == Type.DROP_NULL)
				.map(Step::getOperation)
				.flatMap(operation -> operation.getTables().stream())
				.collect(Collectors.toSet());

		addNullRecords.forEach(table -> {
			String message = "There is no DROP_NULL operation for table: " + table.getName();
			collector.checkThat(message, dropNullRecords, hasItem(table));
			dropNullRecords.remove(table);
		});
		dropNullRecords.forEach(table -> {
			String message = "There is no ADD_NULL operation for table: " + table.getName();
			collector.checkThat(message, addNullRecords, hasItem(table));
		});
	}

	@Test
	public void testThatRemoveNullStepIsLastStepIfAddNullStepsArePresent() {
		boolean containsAddNullSteps = plan.getSteps().stream()
				.map(Step::getOperation)
				.anyMatch(op -> op.getType() == Type.ADD_NULL);

		if (!containsAddNullSteps) {
			return;
		}

		List<Step> stepsWithNoDependees = Lists.newArrayList(plan.getSteps());
		plan.getSteps().forEach(step -> step.getDependencies().forEach(stepsWithNoDependees::remove));

		collector.checkThat("There can only be one last step", stepsWithNoDependees, hasSize(1));

		Operation lastOperation = stepsWithNoDependees.get(0).getOperation();
		collector.checkThat("The last step is not a DROP NULL step", lastOperation.getType(), is(Type.DROP_NULL));
	}

	@Test
	public void testThatAllColumnsAreMigrated() {
		Multimap<Table, String> columns = HashMultimap.create();
		plan.getSteps().stream()
				.map(Step::getOperation)
				.filter(op -> op.getType() == Type.COPY)
				.forEach(op -> op.getTables().forEach(table -> columns.putAll(table, op.getColumns())));

		columns.asMap().forEach((k, v) -> {
			Set<String> columnNames = k.getColumns().stream()
					.map(Column::getName)
					.collect(Collectors.toSet());

			collector.checkThat(columnNames, equalTo(v));
		});
	}

	@Test
	public void testThatIdentityColumnsAreMigratedFirst() {
		for (Step step : plan.getSteps()) {
			Operation operation = step.getOperation();
			if (operation.getType() != Type.COPY) {
				continue;
			}

			Table table = operation.getTables().iterator().next();

			List<Column> identityColumns = table.getIdentityColumns();
			Set<Column> columns = operation.getColumns().stream()
					.map(table::getColumn)
					.collect(Collectors.toSet());

			if (!columns.containsAll(identityColumns)) {
				Set<Step> dependencies = step.getTransitiveDependencies();

				boolean migratesIdentities = false;
				for (Step dependency : dependencies) {
					Operation dependencyOperation = dependency.getOperation();
					if (dependencyOperation.getType() != Type.COPY) {
						continue;
					}

					Table other = dependencyOperation.getTables().iterator().next();
					Set<Column> dependencyColumns = dependencyOperation.getColumns().stream()
							.map(other::getColumn)
							.collect(Collectors.toSet());

					if (other.equals(table) && dependencyColumns.containsAll(identityColumns)) {
						migratesIdentities = true;
						break;
					}
				}

				collector.checkThat("Identities are not migrated first: " + table.getName(), migratesIdentities, is(true));
			}
		}
	}

	@Test
	public void testThatNotNullableForeignKeysAreSatisfiedBeforeInitialCopy() {
		for (Step step : plan.getSteps()) {
			Operation operation = step.getOperation();
			if (operation.getType() != Type.COPY) {
				continue;
			}

			Table table = operation.getTables().iterator().next();

			Set<Table> requiresTables = table.getForeignKeys().stream()
					.filter(ForeignKey::isNotNullable)
					.map(ForeignKey::getReferredTable)
					.distinct()
					.collect(Collectors.toSet());

			for (Table requiresTable : requiresTables) {
				if (!plan.getGhostTables().contains(requiresTable)) {
					continue;
				}

				Set<String> requiredIdentityColumns = table.getForeignKeys().stream()
						.filter(ForeignKey::isNotNullable)
						.filter(fk -> fk.getReferredTable().equals(requiresTable))
						.flatMap(fk -> fk.getReferredColumns().stream())
						.collect(Collectors.toSet());

				Set<Step> dependencies = step.getTransitiveDependencies();
				boolean satisfied = false;
				for (Step dependency : dependencies) {
					Operation dependencyOperation = dependency.getOperation();
					Set<Table> others = dependencyOperation.getTables();
					if (!others.contains(requiresTable)) {
						continue;
					}

					if (dependencyOperation.getType() == Type.ADD_NULL) {
						satisfied = true;
						break;
					}
					else if (dependencyOperation.getType() == Type.COPY) {
						Set<String> dependencyColumns = dependencyOperation.getColumns().stream()
								.map(requiresTable::getColumn)
								.map(Column::getName)
								.collect(Collectors.toSet());

						if (dependencyColumns.containsAll(requiredIdentityColumns)) {
							satisfied = true;
							break;
						}
					}
				}

				collector.checkThat("Identities of parent table: " + requiresTable.getName()
						+ " should be migrated before copying records of: " + table.getName(), satisfied, is(true));
			}
		}
	}

	private String getRefId(String refId) {
		return refLog.getTableRef(changelog.getLastAdded(), refId).getRefId();
	}

	public Table table(String refId) {
		return catalog.getTable(getRefId(refId));
	}

}
