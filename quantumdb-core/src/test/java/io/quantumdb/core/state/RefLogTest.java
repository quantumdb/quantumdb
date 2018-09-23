package io.quantumdb.core.state;

import static io.quantumdb.core.schema.definitions.Column.Hint.IDENTITY;
import static io.quantumdb.core.schema.definitions.TestTypes.bigint;
import static io.quantumdb.core.schema.definitions.TestTypes.varchar;
import static io.quantumdb.core.utils.RandomHasher.generateHash;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import io.quantumdb.core.schema.definitions.Catalog;
import io.quantumdb.core.schema.definitions.Column;
import io.quantumdb.core.schema.definitions.Table;
import io.quantumdb.core.versioning.RefLog;
import io.quantumdb.core.versioning.RefLog.ColumnRef;
import io.quantumdb.core.versioning.RefLog.SyncRef;
import io.quantumdb.core.versioning.RefLog.TableRef;
import io.quantumdb.core.versioning.Version;
import org.junit.Before;
import org.junit.Test;

public class RefLogTest {

	private RefLog refLog;
	private Version version;
	private Catalog catalog;

	@Before
	public void setUp() {
		this.catalog = new Catalog("test-db");
		catalog.addTable(new Table("users")
				.addColumn(new Column("id", bigint(), IDENTITY))
				.addColumn(new Column("name", varchar(255))));

		this.version = new Version(generateHash(), null);
		this.refLog = RefLog.init(catalog, version);
	}

	@Test
	public void testEmptyConstructor() {
		new RefLog();
	}

	@Test
	public void testStaticConstructor() {
		List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

		assertEquals(1, refs.size());
		TableRef ref = refs.get(0);
		assertEquals("users", ref.getName());
		assertEquals("users", ref.getRefId());
		assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
		assertEquals(Sets.newHashSet(version), ref.getVersions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStaticConstructorThrowsExceptionOnNullCatalog() {
		RefLog.init(null, version);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStaticConstructorThrowsExceptionOnNullVersion() {
		RefLog.init(catalog, null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testStaticConstructorThrowsExceptionOnNonRootVersion() {
		Version nextVersion = new Version(generateHash(), version);
		RefLog.init(catalog, nextVersion);
	}

	@Test
	public void testForkingVersion() {
		Version nextVersion = new Version(generateHash(), version);
		refLog.fork(nextVersion);

		List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

		assertEquals(1, refs.size());
		TableRef ref = refs.get(0);
		assertEquals("users", ref.getName());
		assertEquals("users", ref.getRefId());
		assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
		assertEquals(Sets.newHashSet(version, nextVersion), ref.getVersions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testForkingThrowsExceptionOnNullInput() {
		refLog.fork(null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testForkingThrowsExceptionOnRootNode() {
		refLog.fork(version);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testForkingThrowsExceptionOnUnknownNode() {
		Version secondVersion = new Version(generateHash(), version);
		Version thirdVersion = new Version(generateHash(), secondVersion);
		refLog.fork(thirdVersion);
	}

	@Test
	public void testReplacingTable() {
		String refId = generateHash();
		refLog.replaceTable(version, "users", "users", refId);

		List<TableRef> refs = Lists.newArrayList(refLog.getTableRefs());

		assertEquals(1, refs.size());
		TableRef ref = refs.get(0);
		assertEquals("users", ref.getName());
		assertEquals(refId, ref.getRefId());
		assertEquals(ImmutableSet.of("id", "name"), ref.getColumns().keySet());
		assertEquals(Sets.newHashSet(version), ref.getVersions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnNullVersion() {
		String refId = generateHash();
		refLog.replaceTable(null, "users", "users", refId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnNullSourceTableName() {
		String refId = generateHash();
		refLog.replaceTable(version, null, "users", refId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnEmptySourceTableName() {
		String refId = generateHash();
		refLog.replaceTable(version, "", "users", refId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnNullTargetTableName() {
		String refId = generateHash();
		refLog.replaceTable(version, "users", null, refId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnEmptyTargetTableName() {
		String refId = generateHash();
		refLog.replaceTable(version, "users", "", refId);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnNullRefId() {
		refLog.replaceTable(version, "users", "users", null);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testReplacingTableThrowsExceptionOnEmptyRefId() {
		refLog.replaceTable(version, "users", "users", "");
	}

	@Test
	public void testReplacingTableInNextVersion() {
		String refId = generateHash();
		Version nextVersion = new Version(generateHash(), version);
		refLog.fork(nextVersion);
		refLog.replaceTable(nextVersion, "users", "users", refId);

		Map<Version, TableRef> refs = refLog.getTableRefs().stream()
				.collect(Collectors.toMap(ref -> ref.getVersions().stream().findFirst().get(), Function.identity()));

		assertEquals(2, refs.size());

		TableRef ref1 = refs.get(version);
		assertEquals("users", ref1.getName());
		assertEquals("users", ref1.getRefId());
		assertEquals(ImmutableSet.of("id", "name"), ref1.getColumns().keySet());
		assertEquals(Sets.newHashSet(version), ref1.getVersions());

		TableRef ref2 = refs.get(nextVersion);
		assertEquals("users", ref2.getName());
		assertEquals(refId, ref2.getRefId());
		assertEquals(ImmutableSet.of("id", "name"), ref2.getColumns().keySet());
		assertEquals(Sets.newHashSet(nextVersion), ref2.getVersions());

		assertEquals(ImmutableSet.of(ref2.getColumns().get("id")), ref1.getColumns().get("id").getBasisFor());
		assertEquals(ImmutableSet.of(ref1.getColumns().get("id")), ref2.getColumns().get("id").getBasedOn());
		assertEquals(ImmutableSet.of(ref2.getColumns().get("name")), ref1.getColumns().get("name").getBasisFor());
		assertEquals(ImmutableSet.of(ref1.getColumns().get("name")), ref2.getColumns().get("name").getBasedOn());
	}

	@Test
	public void testDroppingEntireTableRef() {
		TableRef tableRef = refLog.getTableRef(version, "users");
		refLog.dropTable(tableRef);

		assertTrue(refLog.getTableRefs().isEmpty());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testDroppingEntireTableRefThrowsExceptionOnNullInput() {
		refLog.dropTable(null);
	}

	@Test
	public void testAddingNewTable() {
		String refId = generateHash();
		refLog.addTable("transactions", refId, version, Lists.newArrayList(
				new ColumnRef("id"),
				new ColumnRef("sender_id"),
				new ColumnRef("receiver_id"),
				new ColumnRef("amount")
		));

		TableRef tableRef = refLog.getTableRef(version, "transactions");
		assertEquals("transactions", tableRef.getName());
		assertEquals(refId, tableRef.getRefId());
		assertEquals(ImmutableSet.of("id", "sender_id", "receiver_id", "amount"), tableRef.getColumns().keySet());
		assertEquals(Sets.newHashSet(version), tableRef.getVersions());
	}

	@Test
	public void testAddingNewTableBasedOnOtherTable() {
		String refId = generateHash();
		TableRef users = refLog.getTableRef(version, "users");
		ImmutableMap<String, ColumnRef> userColumns = users.getColumns();
		Version nextVersion = new Version(generateHash(), version);

		ColumnRef idColumn = new ColumnRef("id", Sets.newHashSet(userColumns.get("id")));
		ColumnRef nameColumn = new ColumnRef("name", Sets.newHashSet(userColumns.get("name")));
		refLog.addTable("users_v2", refId, nextVersion, Lists.newArrayList(idColumn, nameColumn));

		TableRef usersV2 = refLog.getTableRef(nextVersion, "users_v2");
		assertEquals("users_v2", usersV2.getName());
		assertEquals(refId, usersV2.getRefId());
		assertEquals(ImmutableMap.of("id", idColumn, "name", nameColumn), usersV2.getColumns());
		assertEquals(Sets.newHashSet(nextVersion), usersV2.getVersions());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTableWithNullNameThrowsException() {
		refLog.addTable(null, generateHash(), version, Lists.newArrayList());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTableWithEmptyNameThrowsException() {
		refLog.addTable("", generateHash(), version, Lists.newArrayList());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTableWithNullIdThrowsException() {
		refLog.addTable("transactions", null, version, Lists.newArrayList());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTableWithEmptyIdThrowsException() {
		refLog.addTable("transactions", "", version, Lists.newArrayList());
	}

	@Test(expected = IllegalArgumentException.class)
	public void testAddingTableWithNullVersionThrowsException() {
		refLog.addTable("transactions", generateHash(), null, Lists.newArrayList());
	}

	@Test(expected = IllegalStateException.class)
	public void testAddingTableWithExistingNameAndVersionCombinationThrowsException() {
		refLog.addTable("users", generateHash(), version, Lists.newArrayList());
	}

	@Test
	public void testAddingSync() {
		String refId = generateHash();
		Version nextVersion = new Version(generateHash(), version);
		refLog.fork(nextVersion);
		TableRef oldRef = refLog.getTableRef(version, "users");
		TableRef newRef = refLog.replaceTable(nextVersion, "users", "users", refId);

		Map<ColumnRef, ColumnRef> columnMapping = oldRef.getColumns().entrySet().stream()
				.collect(Collectors.toMap(Entry::getValue, entry -> newRef.getColumns().get(entry.getKey())));

		SyncRef syncRef = refLog.addSync(generateHash(), generateHash(), columnMapping);
		assertEquals(oldRef, syncRef.getSource());
		assertEquals(newRef, syncRef.getTarget());
	}

}
