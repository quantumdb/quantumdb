package io.quantumdb.core.versioning;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.persistence.EntityManager;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.Maps;
import com.google.common.collect.Queues;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;
import com.google.inject.persist.Transactional;
import com.mysema.query.jpa.impl.JPAQuery;
import io.quantumdb.core.backends.postgresql.PostgresTypes;
import io.quantumdb.core.schema.definitions.ColumnType;
import io.quantumdb.core.schema.operations.SchemaOperation;
import io.quantumdb.core.utils.RandomHasher;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.reflections.Reflections;

@Slf4j
@Getter(AccessLevel.NONE)
public class ChangelogBackend {

	private final Provider<EntityManager> entityManagerProvider;

	@Inject
	ChangelogBackend(Provider<EntityManager> entityManagerProvider) {
		this.entityManagerProvider = entityManagerProvider;
	}

	@Transactional
	public Changelog load() {
		EntityManager entityManager = entityManagerProvider.get();

		Changelog log = null;
		Gson gson = createSerializer();

		Map<String, RawChangeSet> changeSets = new JPAQuery(entityManager).from(QRawChangeSet.rawChangeSet)
				.orderBy(QRawChangeSet.rawChangeSet.created.asc())
				.list(QRawChangeSet.rawChangeSet).stream()
				.collect(Collectors.toMap(changeSet -> changeSet.getVersion().getId(), Function.identity()));

		List<RawVersion> versions = new JPAQuery(entityManager).from(QRawVersion.rawVersion)
				.list(QRawVersion.rawVersion);

		for (RawVersion version : versions) {
			if (log == null) {
				log = new Changelog(version.getId());
			}
			else {
				Version parent = log.getVersion(version.getParentVersion().getId());
				SchemaOperation schemaOperation = version.getSchemaOperation(gson, SchemaOperation.class);

				RawChangeSet raw = changeSets.get(version.getId());
				ChangeSet changeSet = new ChangeSet(raw.getAuthor(), raw.getCreated(), raw.getDescription());
				log.addChangeSet(parent, version.getId(), changeSet, schemaOperation);
			}
		}

		if (log == null) {
			log = new Changelog(RandomHasher.generateHash());
		}
		return log;
	}

	@Transactional
	public void persist(Changelog changelog) {
		Gson serializer = createSerializer();
		EntityManager entityManager = entityManagerProvider.get();

		Map<String, RawVersion> persistedVersions = new JPAQuery(entityManager).from(QRawVersion.rawVersion)
				.list(QRawVersion.rawVersion).stream()
				.collect(Collectors.toMap(RawVersion::getId, Function.identity()));

		Map<String, RawChangeSet> persistedChangeSets = new JPAQuery(entityManager).from(QRawChangeSet.rawChangeSet)
				.list(QRawChangeSet.rawChangeSet).stream()
				.collect(Collectors.toMap(changeSet -> changeSet.getVersion().getId(), Function.identity()));

		Map<String, RawVersion> processed = Maps.newHashMap();

		Queue<Version> queue = Queues.newLinkedBlockingQueue();
		queue.add(changelog.getRoot());
		while (!queue.isEmpty()) {
			Version version = queue.poll();
			SchemaOperation schemaOperation = version.getSchemaOperation();

			RawVersion rawVersion = persistedVersions.getOrDefault(version.getId(), new RawVersion());
			rawVersion.setId(version.getId());
			rawVersion.setSchemaOperation(serializer.toJson(schemaOperation));
			if (version.getParent() != null) {
				String parentId = version.getParent().getId();
				RawVersion parent = processed.get(parentId);
				rawVersion.setParentVersion(parent);
			}

			if (entityManager.contains(rawVersion)) {
				rawVersion = entityManager.merge(rawVersion);
			}
			else {
				entityManager.persist(rawVersion);
			}

			ChangeSet changeSet = version.getChangeSet();
			if (changeSet != null) {
				RawChangeSet rawChangeSet = persistedChangeSets.getOrDefault(rawVersion.getId(), new RawChangeSet());
				rawChangeSet.setVersionId(rawVersion.getId());
				rawChangeSet.setVersion(rawVersion);
				rawChangeSet.setAuthor(changeSet.getAuthor());
				rawChangeSet.setCreated(changeSet.getCreated());
				rawChangeSet.setDescription(changeSet.getDescription());

				if (entityManager.contains(rawChangeSet)) {
					rawChangeSet = entityManager.merge(rawChangeSet);
				}
				else {
					entityManager.persist(rawChangeSet);
				}

				rawVersion.setChangeSet(rawChangeSet);
			}

			processed.put(rawVersion.getId(), rawVersion);

			Version child = version.getChild();
			if (child != null) {
				queue.add(child);
			}
		}
	}

	private Gson createSerializer() {
		Reflections reflections = new Reflections("io.quantumdb");
		Map<String, Class<? extends SchemaOperation>> operations = reflections.getSubTypesOf(SchemaOperation.class).stream()
				.collect(Collectors.toMap(type -> type.getSimpleName(), Function.identity()));

		JsonDeserializer<ColumnType> columnTypeDeserializer = new JsonDeserializer<ColumnType>() {
			@Override
			public ColumnType deserialize(JsonElement json, Type typeOfT,
					JsonDeserializationContext context) throws JsonParseException {

				JsonObject object = json.getAsJsonObject();
				String type = object.getAsJsonPrimitive("type").getAsString();
				Integer length = null;

				if (type.contains("(")) {
					String part = type.substring(type.indexOf('(') + 1, type.length() - 1);
					length = Integer.parseInt(part);
					type = type.substring(0, type.indexOf('('));
				}

				return PostgresTypes.from(type, length);
			}
		};

		JsonSerializer<ColumnType> columnTypeSerializer = new JsonSerializer<ColumnType>() {
			@Override
			public JsonElement serialize(ColumnType src, Type typeOfSrc, JsonSerializationContext context) {
				JsonObject object = new JsonObject();
				object.addProperty("type", src.getNotation());
				return object;
			}
		};

		Gson defaultSerializer = new GsonBuilder()
				.registerTypeAdapter(ColumnType.class, columnTypeDeserializer)
				.registerTypeAdapter(ColumnType.class, columnTypeSerializer)
				.create();

		log.trace("Creating deserializer for general type: {}", SchemaOperation.class);
		GsonBuilder builder = new GsonBuilder()
				.registerTypeAdapter(ColumnType.class, columnTypeDeserializer)
				.registerTypeAdapter(ColumnType.class, columnTypeSerializer)
				.registerTypeAdapter(SchemaOperation.class, new JsonDeserializer<SchemaOperation>() {
					@Override
					public SchemaOperation deserialize(JsonElement json, Type typeOfT,
							JsonDeserializationContext context) throws JsonParseException {

						JsonObject object = json.getAsJsonObject();
						String type = object.getAsJsonPrimitive("type").getAsString();
						Class<? extends SchemaOperation> operationType = operations.get(type);

						return context.deserialize(json, operationType);
					}
				});

		for (Map.Entry<String, Class<? extends SchemaOperation>> entry : operations.entrySet()) {
			log.trace("Creating serializer for type: {}", entry.getValue());
			builder.registerTypeAdapter(entry.getValue(), new JsonSerializer<SchemaOperation>() {
				@Override
				public JsonElement serialize(SchemaOperation src, Type typeOfSrc, JsonSerializationContext context) {
					JsonObject object = (JsonObject) defaultSerializer.toJsonTree(src);
					object.addProperty("type", entry.getKey());
					return object;
				}
			});
		}

		return builder.create();
	}

}
