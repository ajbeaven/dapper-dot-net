﻿using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Collections.Concurrent;
using System.Reflection.Emit;
using System.Threading;

using Dapper;

#pragma warning disable 1573, 1591 // xml comments

namespace Dapper.Contrib.Extensions
{

	public static partial class SqlMapperExtensions
	{
		// ReSharper disable once MemberCanBePrivate.Global
		public interface IProxy //must be kept public
		{
			bool IsDirty { get; set; }
		}

		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> KeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ManualKeyProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> TypeProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>> ComputedProperties = new ConcurrentDictionary<RuntimeTypeHandle, IEnumerable<PropertyInfo>>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> GetQueries = new ConcurrentDictionary<RuntimeTypeHandle, string>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, string> TypeTableName = new ConcurrentDictionary<RuntimeTypeHandle, string>();
		private static readonly ConcurrentDictionary<RuntimeTypeHandle, bool> TypeTableTablePerType = new ConcurrentDictionary<RuntimeTypeHandle, bool>();

		private static readonly NameOnlyPropertyComparer PropertyComparer = new NameOnlyPropertyComparer();

		private static readonly Dictionary<string, ISqlAdapter> AdapterDictionary = new Dictionary<string, ISqlAdapter> {
																							{"sqlconnection", new SqlServerAdapter()},
																							{"npgsqlconnection", new PostgresAdapter()},
																							{"sqliteconnection", new SQLiteAdapter()}
																						};

		private static IEnumerable<PropertyInfo> ComputedPropertiesCache(Type type)
		{
			IEnumerable<PropertyInfo> pi;
			if (ComputedProperties.TryGetValue(type.TypeHandle, out pi))
			{
				return pi.ToList();
			}

			var computedProperties = TypePropertiesCache(type).Where(p => p.GetCustomAttributes(true).Any(a => a is ComputedAttribute)).ToList();

			ComputedProperties[type.TypeHandle] = computedProperties;
			return computedProperties;
		}

		private static List<PropertyInfo> KeyPropertiesCache(Type type)
		{

			IEnumerable<PropertyInfo> pi;
			if (KeyProperties.TryGetValue(type.TypeHandle, out pi))
			{
				return pi.ToList();
			}

			var allProperties = TypePropertiesCache(type);
			var keyProperties = allProperties.Where(p => p.GetCustomAttributes(true).Any(a => a is KeyAttribute)).ToList();

			//if (keyProperties.Count == 0)
			//{
			//	var idProp = allProperties.FirstOrDefault(p => p.Name.ToLower() == "id");
			//	if (idProp != null)
			//	{
			//		keyProperties.Add(idProp);
			//	}
			//}

			KeyProperties[type.TypeHandle] = keyProperties;
			return keyProperties;
		}
		private static List<PropertyInfo> ManualKeyPropertiesCache(Type type)
		{

			IEnumerable<PropertyInfo> pi;
			if (ManualKeyProperties.TryGetValue(type.TypeHandle, out pi))
			{
				return pi.ToList();
			}

			var allProperties = TypePropertiesCache(type);
			var keyProperties = allProperties.Where(p => p.GetCustomAttributes(true).Any(a => a is ManualKeyAttribute)).ToList();


			ManualKeyProperties[type.TypeHandle] = keyProperties;
			return keyProperties;
		}

		private static List<PropertyInfo> TypePropertiesCache(Type type)
		{
			IEnumerable<PropertyInfo> pis;
			if (TypeProperties.TryGetValue(type.TypeHandle, out pis))
			{
				return pis.ToList();
			}

			var properties = type.GetProperties().Where(IsWriteable).ToArray();

			//Don't include fields from our parent class if we are TablePerType (Do include keys however!)
			if (TypeIsTablePerType(type))
			{
				properties = properties.Except(TypePropertiesCache(type.BaseType), PropertyComparer).Concat(KeyPropertiesCache(type.BaseType)).Concat(ManualKeyPropertiesCache(type.BaseType)).ToArray();
			}

			TypeProperties[type.TypeHandle] = properties;
			return properties.ToList();
		}

		private static bool IsWriteable(PropertyInfo pi)
		{
			var attributes = pi.GetCustomAttributes(typeof(WriteAttribute), false);
			if (attributes.Length != 1) return true;

			var writeAttribute = (WriteAttribute)attributes[0];
			return writeAttribute.Write;
		}

		/// <summary>
		/// Returns a single entity by a single id from table "Ts".  
		/// Id must be marked with [Key] attribute.
		/// Entities created from interfaces are tracked/intercepted for changes and used by the Update() extension
		/// for optimal performance. 
		/// </summary>
		/// <typeparam name="T">Interface or type to create and populate</typeparam>
		/// <param name="connection">Open SqlConnection</param>
		/// <param name="id">Id of the entity to get, must be marked with [Key] attribute</param>
		/// <returns>Entity of T</returns>
		public static T Get<T>(this IDbConnection connection, dynamic id, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			var type = typeof(T);

#if DEBUG
			if (type.IsAbstract)
				throw new Exception("It's really weird to insert an abstract class");
#endif

			string sql;
			if (!GetQueries.TryGetValue(type.TypeHandle, out sql))
			{
				var keys = KeyPropertiesCache(type);
				if (keys.Count() > 1)
					throw new DataException("Get<T> only supports an entity with a single [Key] property");
				if (!keys.Any())
				{
					keys = ManualKeyPropertiesCache(type);

					if (keys.Count() > 1)
						throw new DataException("Get<T> only supports an entity with a single [ManualKey] property");
					if (keys.Count() == 0)
						throw new DataException("Get<T> only supports an entity with a [Key] or [ManualKey] property");
				}

				var onlyKey = keys.First();

				var name = GetTableName(type);

				if (TypeIsTablePerType(type))
				{
					name += " JOIN " + GetTableName(type.BaseType) + " USING (" + onlyKey.Name + ")";
				}

				// TODO: pluralizer 
				// TODO: query information schema and only select fields that are both in information schema and underlying class / interface 
				sql = "select * from " + name + " where " + onlyKey.Name + " = @id";
				GetQueries[type.TypeHandle] = sql;
			}

			var dynParms = new DynamicParameters();
			dynParms.Add("@id", id);

			T obj;

			if (type.IsInterface)
			{
				var res = connection.Query(sql, dynParms).FirstOrDefault() as IDictionary<string, object>;

				if (res == null)
					return null;

				obj = ProxyGenerator.GetInterfaceProxy<T>();

				foreach (var property in TypePropertiesCache(type))
				{
					var val = res[property.Name];
					property.SetValue(obj, val, null);
				}

				((IProxy)obj).IsDirty = false;   //reset change tracking and return
			}
			else
			{
				obj = connection.Query<T>(sql, dynParms, transaction, commandTimeout: commandTimeout).FirstOrDefault();
			}
			return obj;
		}
		private static string GetTableName(Type type)
		{
			string name;
			if (!TypeTableName.TryGetValue(type.TypeHandle, out name))
			{
				name = type.Name + "s";
				if (type.IsInterface && name.StartsWith("I"))
					name = name.Substring(1);

				//NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
				var tableattr = type.GetCustomAttributes(false).SingleOrDefault(attr => attr.GetType().Name == "TableAttribute") as
					dynamic;
				if (tableattr != null)
					name = tableattr.Name;
				TypeTableName[type.TypeHandle] = name;
			}
			return "[" + name + "]";
		}


		private static bool TypeIsTablePerType(Type type)
		{
			bool isTpt;
			if (!TypeTableTablePerType.TryGetValue(type.TypeHandle, out isTpt))
			{
				//NOTE: This as dynamic trick should be able to handle both our own Table-attribute as well as the one in EntityFramework 
				var tableattr = type.GetCustomAttributes(false).SingleOrDefault(attr => attr is TablePerTypeAttribute);
				isTpt = tableattr != null;
				TypeTableTablePerType[type.TypeHandle] = isTpt;
			}
			return isTpt;
		}

		/// <summary>
		/// Inserts an entity into table "Ts" and returns identity id.
		/// </summary>
		/// <param name="connection">Open SqlConnection</param>
		/// <param name="entityToInsert">Entity to insert</param>
		/// <returns>Identity of inserted entity</returns>
		public static long Insert<T>(this IDbConnection connection, T entityToInsert, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			var type = typeof(T);

#if DEBUG
			if (type.IsAbstract)
				throw new Exception("It's really weird to insert an abstract class");
#endif

			if (TypeIsTablePerType(type))
				return TablePerTypeInsert(connection, entityToInsert, transaction, commandTimeout);

			ISqlAdapter adapter = GetFormatter(connection);

			var name = GetTableName(type);

			var keyProperties = KeyPropertiesCache(type);

#if DEBUG
			foreach (var key in keyProperties)
			{
				var value = key.GetValue(entityToInsert, null);
				if (value is int && (int)value != 0)
					throw new Exception("Trying to Insert but the [Id] is not 0");
				else if (value is long && (long)value != 0L)
					throw new Exception("Trying to Insert but the [Id] is not 0");
			}
#endif

			var allProperties = TypePropertiesCache(type);
			var computedProperties = ComputedPropertiesCache(type);
			var allPropertiesExceptKeyAndComputed = allProperties.Except(keyProperties.Union(computedProperties)).ToList();
			var columnList = GenerateColumnList<T>(allPropertiesExceptKeyAndComputed, adapter);
			var parameterList = GenerateParameterList<T>(allPropertiesExceptKeyAndComputed);

			return adapter.Insert(connection, transaction, commandTimeout, name, columnList, parameterList, keyProperties, entityToInsert); ;
		}


		private static string GenerateParameterList<T>(IList<PropertyInfo> propertiesToInclude) where T : class
		{
			var sbParameterList = new StringBuilder(null);
			for (var i = 0; i < propertiesToInclude.Count(); i++)
			{
				var property = propertiesToInclude.ElementAt(i);
				sbParameterList.AppendFormat("@{0}", property.Name);
				if (i < propertiesToInclude.Count() - 1)
					sbParameterList.Append(", ");
			}
			var parameterList = sbParameterList.ToString();
			return parameterList;
		}

		private static string GenerateColumnList<T>(IList<PropertyInfo> propertiesToInclude, ISqlAdapter adapter) where T : class
		{
			var sbColumnList = new StringBuilder(null);
			for (var i = 0; i < propertiesToInclude.Count(); i++)
			{
				var property = propertiesToInclude.ElementAt(i);
				sbColumnList.AppendFormat("[{0}]", property.Name);
				if (i < propertiesToInclude.Count() - 1)
					sbColumnList.Append(", ");
			}

			var columnList = sbColumnList.ToString();
			return columnList;
		}

		private static long TablePerTypeInsert<T>(IDbConnection connection, T entityToInsert, IDbTransaction transaction, int? commandTimeout) where T : class
		{
			var type = typeof(T);
			var parentType = type.BaseType;

			ISqlAdapter adapter = GetFormatter(connection);

			#region Insert the parent class

			var parentName = GetTableName(parentType);
			var parentProperties = TypePropertiesCache(parentType);
			var parentKeyProperties = KeyPropertiesCache(parentType);
			var parentComputedProperties = ComputedPropertiesCache(parentType);
			var parentAllPropertiesExceptKeyAndComputed = parentProperties.Except(parentKeyProperties.Union(parentComputedProperties)).ToArray();

			var parentColumnList = GenerateColumnList<T>(parentAllPropertiesExceptKeyAndComputed, adapter);
			var parentParameterList = GenerateParameterList<T>(parentAllPropertiesExceptKeyAndComputed);

#if DEBUG
			foreach (var key in parentKeyProperties)
			{
				var value = key.GetValue(entityToInsert, null);
				if (value is int && (int)value != 0)
					throw new Exception("Trying to Insert but the [Id] is not 0");
				else if (value is long && (long)value != 0L)
					throw new Exception("Trying to Insert but the [Id] is not 0");
			}
#endif

			long id = adapter.Insert(connection, transaction, commandTimeout, parentName, parentColumnList, parentParameterList, parentKeyProperties, entityToInsert);

			#endregion


			#region Insert the child class

			var name = GetTableName(type);
			var allProperties = TypePropertiesCache(type);
			//Child class shouldn't have a Key directly, it is inherited from the parent and needs to be inserted here
			var computedProperties = ComputedPropertiesCache(type);
			var allPropertiesIncludingKeyExceptComputed = allProperties.Except(computedProperties).ToArray();

			var columnList = GenerateColumnList<T>(allPropertiesIncludingKeyExceptComputed, adapter);
			var parameterList = GenerateParameterList<T>(allPropertiesIncludingKeyExceptComputed);

			adapter.Insert(connection, transaction, commandTimeout, name, columnList, parameterList, Enumerable.Empty<PropertyInfo>(), entityToInsert);

			#endregion

			return id;
		}

		/// <summary>
		/// Updates entity in table "Ts", checks if the entity is modified if the entity is tracked by the Get() extension.
		/// </summary>
		/// <typeparam name="T">Type to be updated</typeparam>
		/// <param name="connection">Open SqlConnection</param>
		/// <param name="entityToUpdate">Entity to be updated</param>
		/// <returns>true if updated, false if not found or not modified (tracked entities)</returns>
		public static bool Update<T>(this IDbConnection connection, T entityToUpdate, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			ISqlAdapter adapter = GetFormatter(connection);
			var proxy = entityToUpdate as IProxy;
			if (proxy != null)
			{
				if (!proxy.IsDirty) return false;
			}

			var type = typeof(T);

#if DEBUG
			if (type.IsAbstract)
				throw new Exception("It's really weird to insert an abstract class");
#endif

			var res = PerformUpdate(connection, entityToUpdate, transaction, commandTimeout, type, adapter);

			if (TypeIsTablePerType(type))
				res &= PerformUpdate(connection, entityToUpdate, transaction, commandTimeout, type.BaseType, adapter);

			return res;
		}

		private static bool PerformUpdate(IDbConnection connection, object entityToUpdate, IDbTransaction transaction, int? commandTimeout, Type type, ISqlAdapter adapter)
		{
			var keyProperties = KeyPropertiesCache(type).Concat(ManualKeyPropertiesCache(type)).ToArray();
			if (!keyProperties.Any())
				throw new ArgumentException("Entity must have at least one [Key] or [ManualKey] property");

			var name = GetTableName(type);

			var sb = new StringBuilder();
			sb.AppendFormat("update {0} set ", name);

			var allProperties = TypePropertiesCache(type);
			var computedProperties = ComputedPropertiesCache(type);
			var nonIdProps = allProperties.Except(keyProperties.Union(computedProperties)).ToList();

			for (var i = 0; i < nonIdProps.Count(); i++)
			{
				var property = nonIdProps.ElementAt(i);
				sb.AppendFormat("[{0}] = @{1}", property.Name, property.Name);
				if (i < nonIdProps.Count() - 1)
					sb.AppendFormat(", ");
			}
			sb.Append(" where ");
			for (var i = 0; i < keyProperties.Count(); i++)
			{
				var property = keyProperties.ElementAt(i);
				sb.AppendFormat("[{0}] = @{1}", property.Name, property.Name);
				if (i < keyProperties.Count() - 1)
					sb.AppendFormat(" and ");
			}
			var updated = connection.Execute(sb.ToString(), entityToUpdate, commandTimeout: commandTimeout, transaction: transaction);
			return updated > 0;
		}

		/// <summary>
		/// Delete entity in table "Ts".
		/// </summary>
		/// <typeparam name="T">Type of entity</typeparam>
		/// <param name="connection">Open SqlConnection</param>
		/// <param name="entityToDelete">Entity to delete</param>
		/// <returns>true if deleted, false if not found</returns>
		public static bool Delete<T>(this IDbConnection connection, T entityToDelete, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			if (entityToDelete == null)
				throw new ArgumentException("Cannot Delete null Object", "entityToDelete");

			var type = typeof(T);

#if DEBUG
			if (type.IsAbstract)
				throw new Exception("It's really weird to insert an abstract class");
#endif

			var res = PerformDelete(connection, entityToDelete, transaction, commandTimeout, type);

			//There may be a cascade relationship here, type REFERENCES basetype, so don't delete from basetype if there isn't any in type
			if (res && TypeIsTablePerType(type))
				res &= PerformDelete(connection, entityToDelete, transaction, commandTimeout, type.BaseType);

			return res;
		}

		private static bool PerformDelete(IDbConnection connection, object entityToDelete, IDbTransaction transaction, int? commandTimeout, Type type)
		{
			var name = GetTableName(type);

			var keyProperties = KeyPropertiesCache(type).Concat(ManualKeyPropertiesCache(type));
			if (keyProperties.Count() == 0)
				throw new ArgumentException("Entity must have at least one [Key] property");

			var sb = new StringBuilder();
			sb.AppendFormat("delete from {0} where ", name);

			for (var i = 0; i < keyProperties.Count(); i++)
			{
				var property = keyProperties.ElementAt(i);
				sb.AppendFormat("[{0}] = @{1}", property.Name, property.Name);
				if (i < keyProperties.Count() - 1)
					sb.AppendFormat(" and ");
			}
			var deleted = connection.Execute(sb.ToString(), entityToDelete, transaction, commandTimeout);
			return deleted > 0;
		}

		/// <summary>
		/// Delete all entities in the table related to the type T.
		/// </summary>
		/// <typeparam name="T">Type of entity</typeparam>
		/// <param name="connection">Open SqlConnection</param>
		/// <returns>true if deleted, false if none found</returns>
		public static bool DeleteAll<T>(this IDbConnection connection, IDbTransaction transaction = null, int? commandTimeout = null) where T : class
		{
			var type = typeof(T);
			var name = GetTableName(type);
			var statement = String.Format("delete from {0}", name);
			var deleted = connection.Execute(statement, null, transaction, commandTimeout);
			return deleted > 0;
		}

		private static ISqlAdapter GetFormatter(IDbConnection connection)
		{
			var name = connection.GetType().Name.ToLower();
			return !AdapterDictionary.ContainsKey(name) ?
				new SqlServerAdapter() :
				AdapterDictionary[name];
		}

		static class ProxyGenerator
		{
			private static readonly Dictionary<Type, object> TypeCache = new Dictionary<Type, object>();

			private static AssemblyBuilder GetAsmBuilder(string name)
			{
				var assemblyBuilder = Thread.GetDomain().DefineDynamicAssembly(new AssemblyName { Name = name },
					AssemblyBuilderAccess.Run);       //NOTE: to save, use RunAndSave

				return assemblyBuilder;
			}

			public static T GetClassProxy<T>()
			{
				// A class proxy could be implemented if all properties are virtual
				//  otherwise there is a pretty dangerous case where internal actions will not update dirty tracking
				throw new NotImplementedException();
			}


			public static T GetInterfaceProxy<T>()
			{
				Type typeOfT = typeof(T);

				object k;
				if (TypeCache.TryGetValue(typeOfT, out k))
				{
					return (T)k;
				}
				var assemblyBuilder = GetAsmBuilder(typeOfT.Name);

				var moduleBuilder = assemblyBuilder.DefineDynamicModule("SqlMapperExtensions." + typeOfT.Name); //NOTE: to save, add "asdasd.dll" parameter

				var interfaceType = typeof(IProxy);
				var typeBuilder = moduleBuilder.DefineType(typeOfT.Name + "_" + Guid.NewGuid(),
					TypeAttributes.Public | TypeAttributes.Class);
				typeBuilder.AddInterfaceImplementation(typeOfT);
				typeBuilder.AddInterfaceImplementation(interfaceType);

				//create our _isDirty field, which implements IProxy
				var setIsDirtyMethod = CreateIsDirtyProperty(typeBuilder);

				// Generate a field for each property, which implements the T
				foreach (var property in typeof(T).GetProperties())
				{
					var isId = property.GetCustomAttributes(true).Any(a => a is KeyAttribute);
					CreateProperty<T>(typeBuilder, property.Name, property.PropertyType, setIsDirtyMethod, isId);
				}

				var generatedType = typeBuilder.CreateType();

				//assemblyBuilder.Save(name + ".dll");  //NOTE: to save, uncomment

				var generatedObject = Activator.CreateInstance(generatedType);

				TypeCache.Add(typeOfT, generatedObject);
				return (T)generatedObject;
			}


			private static MethodInfo CreateIsDirtyProperty(TypeBuilder typeBuilder)
			{
				var propType = typeof(bool);
				var field = typeBuilder.DefineField("_" + "IsDirty", propType, FieldAttributes.Private);
				var property = typeBuilder.DefineProperty("IsDirty",
											   System.Reflection.PropertyAttributes.None,
											   propType,
											   new[] { propType });

				const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.NewSlot | MethodAttributes.SpecialName |
													MethodAttributes.Final | MethodAttributes.Virtual | MethodAttributes.HideBySig;

				// Define the "get" and "set" accessor methods
				var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + "IsDirty",
											 getSetAttr,
											 propType,
											 Type.EmptyTypes);
				var currGetIl = currGetPropMthdBldr.GetILGenerator();
				currGetIl.Emit(OpCodes.Ldarg_0);
				currGetIl.Emit(OpCodes.Ldfld, field);
				currGetIl.Emit(OpCodes.Ret);
				var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + "IsDirty",
											 getSetAttr,
											 null,
											 new[] { propType });
				var currSetIl = currSetPropMthdBldr.GetILGenerator();
				currSetIl.Emit(OpCodes.Ldarg_0);
				currSetIl.Emit(OpCodes.Ldarg_1);
				currSetIl.Emit(OpCodes.Stfld, field);
				currSetIl.Emit(OpCodes.Ret);

				property.SetGetMethod(currGetPropMthdBldr);
				property.SetSetMethod(currSetPropMthdBldr);
				var getMethod = typeof(IProxy).GetMethod("get_" + "IsDirty");
				var setMethod = typeof(IProxy).GetMethod("set_" + "IsDirty");
				typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
				typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);

				return currSetPropMthdBldr;
			}

			private static void CreateProperty<T>(TypeBuilder typeBuilder, string propertyName, Type propType, MethodInfo setIsDirtyMethod, bool isIdentity)
			{
				//Define the field and the property 
				var field = typeBuilder.DefineField("_" + propertyName, propType, FieldAttributes.Private);
				var property = typeBuilder.DefineProperty(propertyName,
											   System.Reflection.PropertyAttributes.None,
											   propType,
											   new[] { propType });

				const MethodAttributes getSetAttr = MethodAttributes.Public | MethodAttributes.Virtual |
													MethodAttributes.HideBySig;

				// Define the "get" and "set" accessor methods
				var currGetPropMthdBldr = typeBuilder.DefineMethod("get_" + propertyName,
											 getSetAttr,
											 propType,
											 Type.EmptyTypes);

				var currGetIl = currGetPropMthdBldr.GetILGenerator();
				currGetIl.Emit(OpCodes.Ldarg_0);
				currGetIl.Emit(OpCodes.Ldfld, field);
				currGetIl.Emit(OpCodes.Ret);

				var currSetPropMthdBldr = typeBuilder.DefineMethod("set_" + propertyName,
											 getSetAttr,
											 null,
											 new[] { propType });

				//store value in private field and set the isdirty flag
				var currSetIl = currSetPropMthdBldr.GetILGenerator();
				currSetIl.Emit(OpCodes.Ldarg_0);
				currSetIl.Emit(OpCodes.Ldarg_1);
				currSetIl.Emit(OpCodes.Stfld, field);
				currSetIl.Emit(OpCodes.Ldarg_0);
				currSetIl.Emit(OpCodes.Ldc_I4_1);
				currSetIl.Emit(OpCodes.Call, setIsDirtyMethod);
				currSetIl.Emit(OpCodes.Ret);

				//TODO: Should copy all attributes defined by the interface?
				if (isIdentity)
				{
					var keyAttribute = typeof(KeyAttribute);
					var myConstructorInfo = keyAttribute.GetConstructor(new Type[] { });
					var attributeBuilder = new CustomAttributeBuilder(myConstructorInfo, new object[] { });
					property.SetCustomAttribute(attributeBuilder);
				}

				property.SetGetMethod(currGetPropMthdBldr);
				property.SetSetMethod(currSetPropMthdBldr);
				var getMethod = typeof(T).GetMethod("get_" + propertyName);
				var setMethod = typeof(T).GetMethod("set_" + propertyName);
				typeBuilder.DefineMethodOverride(currGetPropMthdBldr, getMethod);
				typeBuilder.DefineMethodOverride(currSetPropMthdBldr, setMethod);
			}
		}

		private class NameOnlyPropertyComparer : IEqualityComparer<PropertyInfo>
		{
			public bool Equals(PropertyInfo x, PropertyInfo y)
			{
				return x.Name == y.Name;
			}

			public int GetHashCode(PropertyInfo obj)
			{
				return obj.Name.GetHashCode();
			}
		}
	}
	[AttributeUsage(AttributeTargets.Class)]
	public class TableAttribute : Attribute
	{
		public TableAttribute(string tableName)
		{
			Name = tableName;
		}

		// ReSharper disable once MemberCanBePrivate.Global
		// ReSharper disable once UnusedAutoPropertyAccessor.Global
		public string Name { get; set; }
	}

	// do not want to depend on data annotations that is not in client profile
	[AttributeUsage(AttributeTargets.Property)]
	public class KeyAttribute : Attribute
	{
	}

	/// <summary>
	/// Like Key, but will be inserted with an insert.
	/// </summary>
	[AttributeUsage(AttributeTargets.Property)]
	public class ManualKeyAttribute : Attribute
	{
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class WriteAttribute : Attribute
	{
		public WriteAttribute(bool write)
		{
			Write = write;
		}
		public bool Write { get; private set; }
	}

	[AttributeUsage(AttributeTargets.Property)]
	public class ComputedAttribute : Attribute
	{
	}

	/// <summary>
	/// Place on the child class of a Table Per Class inheritance object to make SqlMapperExtensions split it over multiple tables
	/// http://weblogs.asp.net/manavi/inheritance-mapping-strategies-with-entity-framework-code-first-ctp5-part-2-table-per-type-tpt
	/// </summary>
	[AttributeUsage(AttributeTargets.Class)]
	public class TablePerTypeAttribute : Attribute
	{
	}
}

public partial interface ISqlAdapter
{
	int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, String tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert);
}

public partial class SqlServerAdapter : ISqlAdapter
{
	public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, String tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
	{
		var cmd = String.Format("insert into {0} ({1}) values ({2})", tableName, columnList, parameterList);

		connection.Execute(cmd, entityToInsert, transaction, commandTimeout);

		//NOTE: would prefer to use IDENT_CURRENT('tablename') or IDENT_SCOPE but these are not available on SQLCE
		var r = connection.Query("select @@IDENTITY id", transaction: transaction, commandTimeout: commandTimeout);
		var id = Convert.ToInt32(r.First().id);
		var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
		if (propertyInfos.Any())
			propertyInfos.First().SetValue(entityToInsert, id, null);
		return id;
	}
}

public partial class PostgresAdapter : ISqlAdapter
{
	public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, String tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
	{
		var sb = new StringBuilder();
		sb.AppendFormat("insert into {0} ({1}) values ({2})", tableName, columnList, parameterList);

		// If no primary key then safe to assume a join table with not too much data to return
		var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
		if (!propertyInfos.Any())
			sb.Append(" RETURNING *");
		else
		{
			sb.Append(" RETURNING ");
			var first = true;
			foreach (var property in propertyInfos)
			{
				if (!first)
					sb.Append(", ");
				first = false;
				sb.Append(property.Name);
			}
		}

		var results = connection.Query(sb.ToString(), entityToInsert, transaction, commandTimeout: commandTimeout).ToList();

		// Return the key by assinging the corresponding property in the object - by product is that it supports compound primary keys
		var id = 0;
		foreach (var p in propertyInfos)
		{
			var value = ((IDictionary<string, object>)results.First())[p.Name.ToLower()];
			p.SetValue(entityToInsert, value, null);
			if (id == 0)
				id = Convert.ToInt32(value);
		}
		return id;
	}
}

public partial class SQLiteAdapter : ISqlAdapter
{
	public int Insert(IDbConnection connection, IDbTransaction transaction, int? commandTimeout, String tableName, string columnList, string parameterList, IEnumerable<PropertyInfo> keyProperties, object entityToInsert)
	{
		var cmd = String.Format("insert into {0} ({1}) values ({2})", tableName, columnList, parameterList);

		connection.Execute(cmd, entityToInsert, transaction, commandTimeout);

		var r = connection.Query("select last_insert_rowid() id", transaction: transaction, commandTimeout: commandTimeout);
		var id = (int)r.First().id;
		var propertyInfos = keyProperties as PropertyInfo[] ?? keyProperties.ToArray();
		if (propertyInfos.Any())
			propertyInfos.First().SetValue(entityToInsert, id, null);
		return id;
	}

}
