/**
 * @fileOverview
 * <p>
 * Copyright (C) 2011 by Ross Korsky
 * </p><p>
 * <b>This software is released under the MIT License</b>
 * </p><pre>
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE
 * </pre>
 */

/**
 * The easiest way to think about how
 * to use the operations found here is to compare SQL tables to JS Arrays.
 * @namespace JavaScript data manipulation namespace.
 */
var DataManip = { version: "1.0.0" };
(function(){

/**
 * Pivots records on the given field (member). The structure of the result
 * is controlled by value of the 'flat' argument.
 *
 * @example Example
 * <code>var data = [
 *   { id: 1, name: 'Bob', score: 92 },
 *   { id: 2, name: 'Sam', score: 68 },
 *   { id: 3, name: 'Bob', score: 89 },
 *   { id: 4, name: 'Sam', score: 72 },
 *   { id: 5, name: 'Sam', score: 81 }
 * ];</code>
 *  
 * <code>DataManip.pivot( data, 'name' );</code>
 * >>>
 * <code>{
 *   Bob: [
 *     { id: 1, name: 'Bob', score: 92 },
 *     { id: 3, name: 'Bob', score: 89 }
 *   ],
 *   Sam: [
 *     { id: 2, name: 'Sam', score: 68 },
 *     { id: 4, name: 'Sam', score: 72 },
 *     { id: 5, name: 'Sam', score: 81 }
 *   ]
 * ]</code>
 * @public
 * @memberOf DataManip
 * @param {Array} records records to act on
 * @param {String} field member name to pivot on
 * @param {boolean} [flat=false] controls result structure
 * <dl>
 *  <dt>true <dd>map of values is returned (value selected is the 'last'
 *               one matching)
 *  <dt>false<dd>(default) map of arrays of values is returned.
 * </dl>
 * @returns {Hash} pivoted view of records
 */
function pivot( records, field, flat ) {
  var out = {};
  flat = typeof(flat) == 'undefined' ? false : flat;
  
  if ( !flat ) {
    for( var i = 0, len = records.length; i < len; i ++ ) {
      var r = records[i];
      // skip bad entries - these occur in IE under the additional case 
      // [{},{},] note the trailing ,
      if ( !r ) continue; 
      var a = out[r[field]];
      if ( !a ) {
        a = [];
        out[r[field]] = a;
      }
      a.push( r );
    }
  } else {
    for( var i = 0, len = records.length; i < len; i ++ ) {
      var r = records[i];
      if ( !r ) continue; 
      var a = out[r[field]] = r;
    }
  }
  
  return out;
}

/**
 * Filters records based on the given field name and value.
 * A record must both have a field with the given name and that fields value
 * must match the given value to be selected.The data structure does not need
 * to be consistent (records can have missing or extra fields).
 *
 * @example Example
 * <code>var data = [
 *   { id: 1, name: 'Bob', score: 92 },
 *   { id: 2, name: 'Sam', score: 68 },
 *   { id: 3, name: 'Bob', score: 89 },
 *   { id: 4, name: 'Sam', score: 72 },
 *   { id: 5, name: 'Sam', score: 81 }
 * ];</code>
 * 
 * <code>DataManip.filter( data, 'name', 'Bob' );</code>
 * >>>
 * <code>[
 *   { id: 1, name: 'Bob', score: 92 },
 *   { id: 3, name: 'Bob', score: 89 }
 * ];</code>
 *
 * @public
 * @memberOf DataManip
 * @param {Array} records records to act on
 * @param {String} name of the 'seeking' field
 * @param value seeking field value
 * @returns {Array} filtered set of items
 */
function filter( records, name, value ) {
  var items = [];
  for( var i = 0, len = records.length; i < len; i ++ ) {
    var r = records[i];
    if ( !r ) continue; 
    var a = r[name];
    if ( a && a == value ) items.push( r );
  }
  return items;
}

/**
 * Copy all of the properties in the source objects over to the destination
 * object. It's in-order, to the last source will override properties of the
 * same name in previous arguments.
 * <p>Modified and taken from underscore.js</p>
 * 
 * @public
 * @memberOf DataManip
 * @param obj object to extend
 * @param [params...] objects to extend with
 * @returns obj given (modified)
 */
var extend = function( obj ) {
  for( var i = 1, len = arguments.length; i < len; i ++ ) {
    var source = arguments[1];
    for (var prop in source) obj[prop] = source[prop];
  }
  return obj;
};

/**
 * Builder of Join functions.
 * <br />
 * <dl><lh>Supported joins</lh>
 *   <dt>FROM
 *     <dd>(required) used as the base iterator target, every record is visited.
 *         If occurring more than once treated as CROSS.
 *   <dt>LEFT JOIN
 *     <dd>Return all rows from the left table, even if there are no matches in
 *         the right table.
 *   <dt>INNER JOIN / JOIN
 *     <dd>Return rows when there is at least one match in both tables.
 *   <dt>CROSS JOIN
 *     <dd>Cartesian product of the sets of rows from the joined tables. Each
 *         row from the first table is combined with each row from the second
 *         table and so on.
 * </dl>
 *
 * <ul>
 * <lh>Notice</lh>
 * <li>Directives must be listed in dependency order (if c->b->a then directive
 *     order must be a, b, c). This is true when setting directives using 
 *     either method (calling .parse or setting .directive).
 * <li>Directive arguments can be in any order and be non-contiguous
 * <li>The generated function is completely independent from the builder (once 
 *     compiled changing the selector will not change the selector used by the
 *     compiled function but the new selector will be used by any function as
 *     the product of calling compile again).
 * <li>The directive value 'join' can be one of: from, left, or inner. one, and
 *     only one, 'from' is a required directive
 * <li>Behavior of having multiple from statements is equivalent to performing 
 *     CROSS JOIN. 
 * <li>Mixing CROSS joins with other join types may result in unexpected, but 
 *     deterministic, behavior if/when directive ordering is implemented in the
 *     future.
 * </ul>
 *
 * @class 
 * Used to construct and compile SQL like FROM / JOIN statements into a 
 * javascript function which can then be used for multiple evaluations of similar
 * data sets.
 *
 * @example 
 * <b>Usage example</b>
 *   Create instance: 
 *     <code>var jb = new Join();</code>
 *
 *   Set directives in one of the following ways:
 *     <code>jb.directives = [
 *       { join: 'from' , arg: 0, as: 'score' },
 *       { join: 'left' , arg: 1, as: 'grade'  , on: { 'grade': 'id'  , 'score': 'grade_id' } },
 *       { join: 'inner', arg: 2, as: 'student', on: { 'student': 'id', 'score': 'student_id' } },
 *       { join: 'left' , arg: 3, as: 'test'   , on: { 'test': 'id'   , 'score': 'test_id' } } 
 *     ];</code>
 *   -- OR --
 *     <code>jb.parse(
 *       "FROM arg0 AS score " +
 *       "LEFT JOIN arg1 AS grade   ON grade.id   = score.grade_id " +
 *       "LEFT JOIN arg2 AS student ON student.id = score.student_id " +
 *       "LEFT JOIN arg3 AS test    ON test.id    = score.test_id "
 *     );</code>
 *
 *   Set the selector function:
 *     this selector will result in a set of records containing only test_score less than 70 
 *     <code>jb.selector = function(){ 
 *       if ( this.score.test_score >= 70 ) return null;
 *       return { student_name: this.student.name, grade: this.grade.letter, test_name: this.test.name };
 *     }</code>
 *
 *   Call .compile and either retain the function returned or reference via .exec later
 *
 * <b>Calling the generated function</b>
 *   given lists of simple objects of the same name as aliased in the directive
 *   example call
 *     <code>var records = jb.exec( scores, grades, students, tests )</code>
 *
 * 
 * @property {Array} directives 
 * Directives are used to generate the javascript which will perform the join.
 * <p>
 * The directives content is considered 'trusted' and therefore directives are
 * a possible JS injection vector (sanitize user input before directly
 * creating directives).
 * </p><p>
 * The parse method may be used to create directives for you by using an SQL
 * fragment like syntax. See: {@link Join#parse}
 * </p><p>
 * 
 * <dl>
 * <lh>Directives have the following structure</lh>
 * <dt>join<dd>one of 'from', 'left', 'inner', 'cross'
 * <dt>arg<dd>index of the function parameter which will be referenced by the
 *     alias established by 'as'. Does not need to be unique.
 * <dt>as<dd>Alias which will be used to refer to 'this' view of the specified
 *     function argument.
 * <dt>on<dd>Structure containing exactly two members of the form 
 *     &lt;alias&gt;: &lt;field&gt;, one alias MUST be the same value as 'as'.
 * </dl>
 * <b>Example</b>
 * <pre class='code'>[
 *   { join: 'from', arg: 0, as: 'score' },
 *   { join: 'left', arg: 1, as: 'grade'  , on: { 'grade': 'id'  , 'score': 'grade_id' } },
 *   { join: 'left', arg: 2, as: 'student', on: { 'student': 'id', 'score': 'student_id' } },
 *   { join: 'left', arg: 3, as: 'test'   , on: { 'test': 'id'   , 'score': 'test_id' } } 
 * ]</pre>
 * </p>
 * <ul>
 * <lh>Notice</lh>
 * <li>arg#'s may be reused just as you can join a table on itself in SQL
 * <li>aliases must be unique within a set of directives
 * <li>only equality '=' is supported at this time
 * <li>having a FROM statement is required
 * <li>joins must have an ON statement that contains reference to the name
 *     aliased by 'as'
 * <li>The parse method will throw Error's for common failure cases but this
 *     error checking may not catch all invalid cases.
 * <li>Currently directives are not reordered by this system and the order 
 *     of directives is used to build the join function code. This means that, 
 *     for now, join dependency MUST be satisfied by the user by the order 
 *     of listing.
 * </ul>
 * 
 * @property {String} __code last generated JavaScript code
 * @memberOf DataManip
 */
function Join() {
  this.__code = undefined;
  this.directives = [];
  return;
}

/**
 * Last generated function, compile must be called first
 * exec.external.selector holds the selector used by this function.
 *
 * @see Join#compile
 *
 * @public
 * @memberOf DataManip.Join.prototype
 * @returns {Array} accumulation of results produced by the selector
 */
Join.prototype.exec = function() {
  throw new Error( "must call compile first" );
}

/**
 * End of line sequence used in generated code.
 *
 * @memberOf DataManip.Join.prototype
 * @default empty string
 */
Join.prototype.__eol__ = '';

/**
 * <b>The user should overwrite this function</b> the default selector simply 
 * returns the full set of records ('this').
 * <p>
 * The selector function like a combination of an SQL SELECT clause and WHERE 
 * clause. It is used by the compiled join function to perform record selection
 * as well as creating a record 'view' or transformation.
 * </p><p>
 * The selector function is called in the context of the current, joined, row.
 * In other words this function references 'tables' by their alias through the
 * 'this' object. For example <code>this.score</code> will refer to the record
 * that was declared to have the alias of 'score' (via 'as') in the directives.
 * </p>
 * <b>Performing 'WHERE' like actions</b>
 * <p>
 *   The selector function is capable of handling 'where' clauses. By 
 *   returning null or undefined the record will be omitted from the result.
 * </p><p>
 * Take note that the return value from the selector is pushed into an array
 * and that array is what is returned from the compiled join function upon
 * its invocation.
 * <ul>
 *   <lh>This has several powerful implications such as...</lh>
 *   <li>Returning 'this' causes the result record to have the same structure as
 *       'this' ('this' is cloned and pushed to the result set).
 *   <li>Returning a contrived anonymous object results in a set of records
 *       having whatever structure that object defines (e.g. <code>return {name: 
 *       this.user.name, group: this.group.name};</code>)
 *   <li>Returning a string or other simple value causes the result record set 
 *       to simply be a list of these primitives.
 *   <li>Returning null or undefined results in no value being pushed to the
 *       result set for this data combination.
 *   <li>Complex actions such as DOM manipulation can occur within the select
 *       function in addition to, or instead of, actually building a list of
 *       records.
 * </ul>
 * </p><p>
 * The selector may be changed after the function is compiled by setting the
 * bound selector at foo.external.selector where foo is a compiled join 
 * function. Changing the selector on a Join object will not affect any
 * previously compiled join functions.
 * </p>
 *
 * @public
 * @memberOf DataManip.Join.prototype
 * @returns {Object} Representation of set to capture or null to ignore
 * this join set.
 */
Join.prototype.selector = function() { return this; }

/**
 * Generates the code to perform the join specified by the directives and
 * returns a compiled function. Also sets this same function as the member
 * this.exec
 * <p>
 * The compiled function should be called passing a set of arrays matching 
 * the order specified by the 'arg: #' value of each directive.
 * </p><p>
 * Changing the members of exec.external is allowed and makes it possible to
 * change the select function used by the compiled join function.
 * </p>
 * @example
 *    given directives = [
 *       { join: 'from', arg: 0, as: 'score' },
 *       { join: 'left', arg: 1, as: 'grade'  , on: { 'grade': 'id'  , 'score': 'grade_id' } },
 *       { join: 'left', arg: 2, as: 'student', on: { 'student': 'id', 'score': 'student_id' } },
 *       { join: 'left', arg: 3, as: 'test'   , on: { 'test': 'id'   , 'score': 'test_id' } } 
 *     ];
 *
 *   given lists of simple objects of the same name as aliased in the directive example
 *   var records = jb.exec( scores, grades, students, tests );
 *
 * @public
 * @memberOf DataManip.Join.prototype
 * @returns {Function} function, that when invoked, will perform the join 
 *          operation specified by the directives as declared.
 */
Join.prototype.compile = function() {
  this.__code = Join__generate.call( this );
  // functions/objects  placed within 'external' are made available 
  // runtime (swappable) via exec.external.* this allows for the selector
  // function to be changed after this call to compile
  var context = {
    external: { 
      selector: this.selector
    },
    extend: extend,
    pivot: pivot
  }
  
  this.exec = (function( ctx, func ) {
    var foo = function() {
      return func.apply( ctx, arguments );
    }
    foo.external = ctx.external;
    return foo;
  })( context, new Function( this.__code ) );
  
  return this.exec;
}

/**
 * Generates the javascript code to perform arbitrary join operations 
 * 
 * @private
 * @memberOf DataManip.Join.prototype
 * @returns {String} JavaScript code
 */
var Join__generate = function() {
  var argMap = {};
  var lookupMap = {};
  var code = '';
  var eol = this.__eol__;
  
  // emit function aliasing
  code += 'var selector=this.external.selector,' + eol;
  code +=     'extend=this.extend,' + eol;
  code +=     'pivot=this.pivot;' + eol;
  
  // emit other members
  code += 'var r=[],' + eol; //records
  code +=     'x={};' + eol; //context
  
  // build list of arguments and lookups needed
  for( var i = 0, len = this.directives.length; i < len; i ++ ) {
    var directive = this.directives[i];
    if ( !directive  ) continue; 
    var argName = 'arg' + directive.arg;
    argMap[argName] = argName + '=arguments[' + directive.arg + ']||[]';
    if ( directive.join != 'from' && directive.join != 'cross') {
      var indexField = directive.on[ directive.as ];
      var lookupName = 'p_' + argName + '_' + indexField;
      lookupMap[lookupName] = lookupName + '=' 
        + 'pivot(' + argName + ',"' + indexField + '")';
    }
  }
  // emit argument uptake and lookup generation
  var j = 0;
  for( var i in argMap ) {
    if ( j++ == 0 ) code += 'var ';
    else code += eol + ',';
    code += argMap[i];
  }
  if ( j > 0 ) code += ';' + eol;
  
  j = 0;
  for( var i in lookupMap ) {
    if ( j++ == 0 ) code += 'var ';
    else code += eol + ',';
    code += lookupMap[i];
  }
  if ( j > 0 ) code += ';' + eol;

  // footers are used to 'close' the for loops
  var footers = [];
  
  // generate loop structure to perform join operation
  for( var i = 0, len = this.directives.length; i < len; i ++ ) {
    var directive = this.directives[i];
    if ( !directive  ) continue; 
    var valuelist_var = 'v' + i;
    var itr_var = 'i' + i;
    var itr_len_var = 'L' + i;
    
    if ( directive.join == 'left' ) {
      code += 'var ' + valuelist_var + '=' 
           + Join__getLookupCommand( directive ) + '||[{}];' + eol;
    } else if ( directive.join == 'inner' ) {
      code += 'var ' + valuelist_var + '=' 
           + Join__getLookupCommand( directive ) + '||[];' + eol;
    } else if ( directive.join == 'from' || directive.join == 'cross' ) {
      var argName = 'arg' + directive.arg;
      code += 'var ' + valuelist_var + '=' + argName + ';' + eol;
    }
    code += 'for(var ' + itr_var + '=0,' + itr_len_var + '='
         + valuelist_var + '.length;'
         + itr_var + '<' + itr_len_var + ';'
         + itr_var + '++){' + eol;
    code += 'x["' + directive.as + '"]=' 
         + valuelist_var + '[' + itr_var + '];' + eol;

    // TODO: delete can be removed once directive dependency ordering is ensured
    footers.push( 'delete(x["' + directive.as + '"]);' + eol + '}' + eol );
  }
  
  // in the deepest loop, emit selector execution
  code += 'var sr=selector.call(x);' + eol;
  code += 'if(sr==x)sr=extend({},x);' + eol;
  code += 'if(sr)r.push(sr);' + eol;
  
  // emit loop closing 'footers'
  while( footers.length > 0 ) {
    code += footers.pop();
  }
  
  // emit return statement - duh
  code += 'return r;';
  return code;
}

/**
 * Computes the right hand side of an index based value retrieval
 * for example the RHS of this statement: var joinWithMeList = lookup[..];
 * 
 * @private
 * @memberOf DataManip.Join
 * @param directive 
 * @returns {String} generated lookup access command
 */
function Join__getLookupCommand( directive ) {
  if ( !directive.on ) return null;
  var argName = 'arg' + directive.arg;
  var indexField = directive.on[ directive.as ];
  var lookupName = 'p_' + argName + '_' + indexField;
  for ( var i in directive.on ) {
    if ( i != directive.as ) 
      return lookupName + '[x["' + i + '"]["' + directive.on[i] + '"]]';
  }
}


/**
 * Parses an SQL like FROM .. JOIN query fragment
 *
 * <dl>
 *   <lh>Groups</lh>
 *   <td>1<dd>function (from, left join, inner join, cross join, join)
 *   <td>2<dd>argument number (numeric part of: arg0, arg1, ... 0, 1, ..,)
 *   <td>3<dd>alias (RHS of AS)
 *   <td>4<dd>join on LHS alias
 *   <td>5<dd>join on LHS field
 *   <td>6<dd>comparison operation (currently can only be '=')
 *   <td>7<dd>join on RHS alias
 *   <td>8<dd>join on RHS field
 * </dl>
 * @private
 */
var sqlFromJoinRegex = /(FROM|LEFT\s+JOIN|INNER\s+JOIN|CROSS JOIN|JOIN)\s+arg(\d+)\s+AS\s+([$a-zA-Z_][$a-zA-Z0-9_]*)\b(?:\s+ON\s+([$a-zA-Z_][$a-zA-Z0-9_]*)\.([$a-zA-Z_][$a-zA-Z0-9_]*)\s*(=)\s*([$a-zA-Z_][$a-zA-Z0-9_]*)\.([$a-zA-Z_][$a-zA-Z0-9_]*))?/ig;

/**
 * The parse() method allows you to specify directives in a, human friendly,
 * SQL like syntax consisting of the FROM and JOIN portions of an SQL
 * statement.
 * <p>
 * The 'SELECT' and 'WHERE' parts are handled by the 'selector' user function.
 * </p>
 * @notice
 *    @see Join#directives
 *  
 * @example statement which may be passed to parse():
 *   FROM arg0 AS score
 *   LEFT JOIN arg1 AS grade   ON grade.id   = score.grade_id
 *   LEFT JOIN arg2 AS student ON student.id = score.student_id
 *   LEFT JOIN arg3 AS test    ON test.id    = score.test_id
 *  
 * @public
 * @memberOf DataManip.Join.prototype
 * @param {String} joinStr SQL join style string to parse
 * @returns {Join} self for chaining
 */
Join.prototype.parse = function( joinStr ) {
  this.directives = [];
  var match;
  var haveFromStatment = false;
  var aliasReuseCheck = {};
  
  while ( match = sqlFromJoinRegex.exec( joinStr ) ) {
    var join = match[1].toLowerCase();
    if ( /^(join|inner\s+join)$/.test( join ) ) join = 'inner';
    if ( /^left\s+join$/.test( join ) ) join = 'left';
    if ( /^cross\s+join$/.test( join ) ) join = 'cross';
    if ( join == 'from' ) {
      if ( haveFromStatment ) join = 'cross';
      haveFromStatment = true;
    }
    var directive = {
      join: join,
      arg: parseInt( match[2] ),
      as: match[3]
    };
    
    if ( aliasReuseCheck[directive.as] ) {
      throw new Error( "directive " + this.directives.length + " - alias '" 
      + directive.as + "' cannot be used more than once" );
    }
    aliasReuseCheck[directive.as] = true;
    
    if ( !/from|cross/.test( join ) ) {
      if ( !match[4] || !match[5] || !match[7] || !match[8] )
        throw new Error( "directive " + this.directives.length 
        + " - ON statment is missing or incorrectly formatted" );
      if ( match[4] != directive.as && match[7] != directive.as )
        throw new Error( "directive " + this.directives.length 
        + " - ON statment must include a field from " + directive.as );
      if ( match[4] == match[7] )
        throw new Error( "directive " + this.directives.length 
        + " - ON statment must include a field not from " + directive.as );
      
      directive.on = {};
      directive.on[ match[4] ] = match[5];
      directive.on[ match[7] ] = match[8];
    }
    this.directives.push( directive );
  }
  
  if ( !haveFromStatment ) {
    throw new Error( "FROM statement is required" );
  }
  return this;
}

DataManip.pivot = pivot;
DataManip.filter = filter;
DataManip.extend = extend;
DataManip.Join = Join;
if ( !window.DataJoin ) window.DataJoin = Join;

})();
