// Copyright 2019 PingCAP, Inc.

use std::fs::{self, File};
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;

use proc_macro2::Span;
use quote::ToTokens;
use syn::{
    Attribute, GenericArgument, Ident, Item, ItemEnum, ItemStruct, Meta, NestedMeta, PathArguments,
    Token, Type, TypePath,
};

use crate::GenOpt;

pub struct WrapperGen {
    input: String,
    input_file: PathBuf,
    gen_opt: GenOpt,
}

impl WrapperGen {
    pub fn new(file_name: PathBuf, gen_opt: GenOpt) -> WrapperGen {
        let input = String::from_utf8(
            fs::read(&file_name).unwrap_or_else(|_| panic!("Could not read {:?}", file_name)),
        )
        .expect("File not utf8");
        WrapperGen {
            input,
            gen_opt,
            input_file: file_name,
        }
    }

    pub fn write(&self) {
        let mut path = self.input_file.clone();
        path.set_file_name(format!(
            "wrapper_{}",
            path.file_name().unwrap().to_str().unwrap()
        ));
        let mut out = BufWriter::new(File::create(&path).expect("Could not create file"));
        self.generate(&mut out).expect("Error generating code");
    }

    fn generate<W>(&self, buf: &mut W) -> Result<(), io::Error>
    where
        W: Write,
    {
        let file = ::syn::parse_file(&self.input).expect("Could not parse file");
        writeln!(buf, "// Generated file, please don't edit manually.\n")?;
        generate_from_items(&file.items, self.gen_opt, "", buf)
    }
}

fn generate_from_items<W>(
    items: &[Item],
    gen_opt: GenOpt,
    prefix: &str,
    buf: &mut W,
) -> Result<(), io::Error>
where
    W: Write,
{
    for item in items {
        if let Item::Struct(item) = item {
            if is_message(&item.attrs) {
                generate_struct(item, gen_opt, prefix, buf)?;
            }
        } else if let Item::Enum(item) = item {
            if is_enum(&item.attrs) {
                generate_enum(item, prefix, buf)?;
            }
        } else if let Item::Mod(m) = item {
            if let Some(ref content) = m.content {
                let prefix = format!("{}{}::", prefix, m.ident);
                generate_from_items(&content.1, gen_opt, &prefix, buf)?;
            }
        }
    }
    Ok(())
}

fn generate_struct<W>(
    item: &ItemStruct,
    gen_opt: GenOpt,
    prefix: &str,
    buf: &mut W,
) -> Result<(), io::Error>
where
    W: Write,
{
    writeln!(buf, "impl {}{} {{", prefix, item.ident)?;
    if gen_opt.contains(GenOpt::NEW) {
        generate_new(&item.ident, prefix, buf)?;
    }
    generate_default_ref(&item.ident, prefix, gen_opt, buf)?;
    item.fields
        .iter()
        .filter_map(|f| {
            f.ident
                .as_ref()
                .map(|i| (i, &f.ty, FieldKind::from_attrs(&f.attrs, prefix)))
        })
        .filter_map(|(n, t, (k, deprecated))| k.methods(t, n, deprecated))
        .map(|m| m.write_methods(buf, gen_opt))
        .collect::<Result<Vec<_>, _>>()?;
    writeln!(buf, "}}")?;
    if gen_opt.contains(GenOpt::MESSAGE) {
        generate_message_trait(&item.ident, prefix, buf)?;
    }
    Ok(())
}

fn generate_enum<W>(item: &ItemEnum, prefix: &str, buf: &mut W) -> Result<(), io::Error>
where
    W: Write,
{
    writeln!(buf, "impl {}{} {{", prefix, item.ident)?;
    writeln!(buf, "pub fn values() -> &'static [Self] {{")?;
    writeln!(
        buf,
        "static VALUES: &'static [{}{}] = &[",
        prefix, item.ident
    )?;
    for v in &item.variants {
        writeln!(buf, "{}{}::{},", prefix, item.ident, v.ident)?;
    }
    writeln!(buf, "];\nVALUES\n}}")?;
    writeln!(buf, "}}")
}

fn generate_new<W>(name: &Ident, prefix: &str, buf: &mut W) -> Result<(), io::Error>
where
    W: Write,
{
    writeln!(
        buf,
        "pub fn new_() -> {}{} {{ ::std::default::Default::default() }}",
        prefix, name,
    )
}

fn generate_default_ref<W>(
    name: &Ident,
    prefix: &str,
    gen_opt: GenOpt,
    buf: &mut W,
) -> Result<(), io::Error>
where
    W: Write,
{
    if gen_opt.contains(GenOpt::MESSAGE) {
        writeln!(
            buf,
            "#[inline] pub fn default_ref() -> &'static Self {{ ::protobuf::Message::default_instance() }}",
        )
    } else {
        writeln!(
            buf,
            "#[inline] pub fn default_ref() -> &'static Self {{
                ::lazy_static::lazy_static! {{
                    static ref INSTANCE: {0}{1} = {0}{1}::default();
                }}
                &*INSTANCE
            }}",
            prefix, name,
        )
    }
}

fn generate_message_trait<W>(name: &Ident, prefix: &str, buf: &mut W) -> Result<(), io::Error>
where
    W: Write,
{
    write!(buf, "impl ::protobuf::Message for {}{} {{", prefix, name)?;
    writeln!(buf, "const NAME: &'static str = \"\";",)?;
    writeln!(
        buf,
        "fn compute_size(&self) -> u64 {{ ::prost::Message::encoded_len(self) as u64 }}",
    )?;
    writeln!(buf, "fn new() -> Self {{ Self::default() }}",)?;
    writeln!(
        buf,
        "fn default_instance() -> &'static {}{} {{
        ::lazy_static::lazy_static! {{
            static ref INSTANCE: {0}{1} = {0}{1}::default();
        }}
        &*INSTANCE
    }}",
        prefix, name,
    )?;
    writeln!(buf, "fn is_initialized(&self) -> bool {{ true }}",)?;
    writeln!(
        buf,
        "fn write_to_with_cached_sizes(&self, _os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::Result<()> {{ unimplemented!(); }}",
    )?;
    writeln!(
        buf,
        "fn merge_from(&mut self, _is: &mut ::protobuf::CodedInputStream) -> ::protobuf::Result<()> {{ unimplemented!(); }}",
    )?;
    writeln!(
        buf,
        "fn special_fields(&self) -> &::protobuf::SpecialFields {{ unimplemented!(); }}",
    )?;
    writeln!(
        buf,
        "fn mut_special_fields(&mut self) -> &mut ::protobuf::SpecialFields {{ unimplemented!(); }}",
    )?;
    writeln!(
        buf,
        "fn write_to_bytes(&self) -> ::protobuf::Result<Vec<u8>> {{
            let mut buf = Vec::new();
            if ::prost::Message::encode(self, &mut buf).is_err() {{
                return Err(::std::io::Error::new(::std::io::ErrorKind::Other, \"encode error\").into());
            }}
            Ok(buf)
        }}"
    )?;
    writeln!(
        buf,
        "fn merge_from_bytes(&mut self, bytes: &[u8]) -> ::protobuf::Result<()> {{
            if ::prost::Message::merge(self, bytes).is_err() {{
                return Err(::std::io::Error::new(::std::io::ErrorKind::Other, \"merge error\").into());
            }}
            Ok(())
        }}"
    )?;
    writeln!(buf, "}}")
}

const INT_TYPES: [&str; 4] = ["int32", "int64", "uint32", "uint64"];

#[derive(Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
enum FieldKind {
    Optional(Box<FieldKind>),
    Repeated {
        // A module prefix of a repeated message.
        prefix: String,
    },
    Message,
    Int,
    Float,
    Bool,
    Bytes,
    String,
    OneOf(String),
    Enumeration(String),
    Map,
    // Fixed are not handled.
}

impl FieldKind {
    fn from_attrs(attrs: &[Attribute], prefix: &str) -> (FieldKind, bool) {
        let mut deprecated = false;

        for a in attrs {
            // condition: in prost generated code, `[deprecated]` appears before `[prost(..)]`
            deprecated = deprecated || a.path.is_ident("deprecated");
            if a.path.is_ident("prost") {
                if let Ok(Meta::List(list)) = a.parse_meta() {
                    let mut kinds = list
                        .nested
                        .iter()
                        .filter_map(|item| {
                            if let NestedMeta::Meta(Meta::Path(id)) = item {
                                if id.is_ident("optional") {
                                    Some(FieldKind::Optional(Box::new(FieldKind::Message)))
                                } else if id.is_ident("message") {
                                    Some(FieldKind::Message)
                                } else if id.is_ident("repeated") {
                                    Some(FieldKind::Repeated {
                                        prefix: prefix.to_owned(),
                                    })
                                } else if id.is_ident("bytes") {
                                    Some(FieldKind::Bytes)
                                } else if id.is_ident("string") {
                                    Some(FieldKind::String)
                                } else if id.is_ident("bool") {
                                    Some(FieldKind::Bool)
                                } else if id.is_ident("float") || id.is_ident("double") {
                                    Some(FieldKind::Float)
                                } else if let Some(id) = id.get_ident() {
                                    if INT_TYPES.contains(&id.to_string().as_str()) {
                                        Some(FieldKind::Int)
                                    } else {
                                        None
                                    }
                                } else {
                                    None
                                }
                            } else if let NestedMeta::Meta(Meta::NameValue(mnv)) = item {
                                let value = mnv.lit.clone().into_token_stream().to_string();
                                // Trim leading and trailing `"` and add prefix.
                                let value = format!("{}{}", prefix, &value[1..value.len() - 1]);
                                if mnv.path.is_ident("bytes") {
                                    Some(FieldKind::Bytes)
                                } else if mnv.path.is_ident("enumeration") {
                                    Some(FieldKind::Enumeration(value))
                                } else if mnv.path.is_ident("oneof") {
                                    Some(FieldKind::OneOf(value))
                                } else if mnv.path.is_ident("map") {
                                    Some(FieldKind::Map)
                                } else {
                                    None
                                }
                            } else {
                                None
                            }
                        })
                        .collect::<Vec<_>>();
                    kinds.sort();
                    if !kinds.is_empty() {
                        let mut iter = kinds.into_iter();
                        let mut result = iter.next().unwrap();
                        // If the type is an optional, keep looking to find the underlying type.
                        if let FieldKind::Optional(_) = result {
                            result = FieldKind::Optional(Box::new(iter.next().unwrap()));
                        }
                        return (result, deprecated);
                    }
                }
            }
        }
        unreachable!("Unknown field kind");
    }

    fn methods(&self, ty: &Type, ident: &Ident, deprecated: bool) -> Option<FieldMethods> {
        let mut result = FieldMethods::new(ty, ident, deprecated);
        match self {
            FieldKind::Optional(fk) => {
                let unwrapped_type = unwrap_type(ty, "Option");
                let unboxed_type = unwrap_type(&unwrapped_type, "Box");
                let nested_methods = fk.methods(&unwrapped_type, ident, deprecated).unwrap();
                let unwrapped_type = unwrapped_type.into_token_stream().to_string();
                let unboxed_type = unboxed_type.into_token_stream().to_string();

                result.override_ty = Some(match nested_methods.override_ty {
                    Some(t) => t,
                    None => unwrapped_type.clone(),
                });
                result.ref_ty = nested_methods.ref_ty;
                result.enum_set = nested_methods.enum_set;
                result.has = true;
                result.clear = Some("::std::option::Option::None".to_owned());
                result.set = Some(match &**fk {
                    FieldKind::Enumeration(_) => "::std::option::Option::Some(v as i32)".to_owned(),
                    _ => "::std::option::Option::Some(v)".to_owned(),
                });

                let as_ref = match &result.ref_ty {
                    RefType::Ref | RefType::Deref(_) => {
                        let unwrapped_type = match &**fk {
                            FieldKind::Bytes | FieldKind::Repeated { .. } => "::std::vec::Vec",
                            _ => &unwrapped_type,
                        };
                        result.mt = MethodKind::Custom(format!(
                            "if self.{}.is_none() {{
                                self.{0} = ::std::option::Option::Some({1}::default());
                            }}
                            self.{0}.as_mut().unwrap()",
                            result.name,
                            type_in_expr_context(unwrapped_type),
                        ));
                        ".as_ref()"
                    }
                    RefType::Copy => "",
                };

                let init_val = match &**fk {
                    FieldKind::Message => {
                        result.take = Some(format!(
                            "self.{}.take().unwrap_or_else({}::default)",
                            result.name,
                            type_in_expr_context(&unwrapped_type),
                        ));
                        format!("{}::default_ref()", type_in_expr_context(&unboxed_type))
                    }
                    FieldKind::Bytes => {
                        result.take = Some(format!(
                            "self.{}.take().unwrap_or_else(::std::vec::Vec::new)",
                            result.name,
                        ));
                        "&[]".to_owned()
                    }
                    FieldKind::String => {
                        result.take = Some(format!(
                            "self.{}.take().unwrap_or_else(::std::string::String::new)",
                            result.name,
                        ));
                        "\"\"".to_owned()
                    }
                    FieldKind::Int | FieldKind::Enumeration(_) => "0".to_owned(),
                    FieldKind::Float => "0.".to_owned(),
                    FieldKind::Bool => "false".to_owned(),
                    _ => unimplemented!(),
                };

                result.get = Some(match &**fk {
                    FieldKind::Enumeration(t) => format!(
                        "match self.{} {{
                            Some(v) => match <{} as ::std::convert::TryFrom<i32>>::try_from(v) {{\
                                Ok(e) => e,
                                Err(_) => panic!(\"Unknown enum variant: {{}}\", v),
                            }},
                            None => {}::default(),
                        }}",
                        result.name,
                        type_in_expr_context(t),
                        t,
                    ),
                    _ => format!(
                        "match self.{}{} {{
                            Some(v) => v,
                            None => {},
                        }}",
                        result.name, as_ref, init_val,
                    ),
                });
            }
            FieldKind::Message => {
                let unboxed_type = unwrap_type(ty, "Box");
                let ty_str = ty.into_token_stream().to_string();
                let unboxed_str = unboxed_type.into_token_stream().to_string();
                if ty_str != unboxed_str {
                    result.ref_ty = RefType::Deref(unboxed_str);
                }
            }
            FieldKind::Int => {
                result.ref_ty = RefType::Copy;
                result.clear = Some("0".to_owned());
            }
            FieldKind::Float => {
                result.ref_ty = RefType::Copy;
                result.clear = Some("0.".to_owned());
            }
            FieldKind::Bool => {
                result.ref_ty = RefType::Copy;
                result.clear = Some("false".to_owned());
            }
            FieldKind::Repeated { prefix } => {
                result.mt = MethodKind::Standard;
                result.take = Some(format!(
                    "::std::mem::replace(&mut self.{}, ::std::vec::Vec::new())",
                    result.name
                ));
                let mut unwrapped_type: &str =
                    &unwrap_type(ty, "Vec").into_token_stream().to_string();

                // The point of the prefix is to account for nesting of modules. However, it turns out
                // that unwrapped_type may start with `super` so if we just smoosh the two together we
                // get an invalid type. So the following small nightmare of code pops a suffix of
                // prefix for every `super`, while there are both `super`s and segments of the prefix.
                let mut segments: Vec<_> = if ["bool", "u32", "i32", "u64", "i32", "f32", "f64"]
                    .contains(&unwrapped_type)
                {
                    // A built-in type should never be prefixed.
                    Vec::new()
                } else {
                    prefix.split("::").collect()
                };
                while let Some(s) = segments.pop() {
                    if s.is_empty() {
                        continue;
                    }
                    if !unwrapped_type.starts_with("super ::") {
                        segments.push(s);
                        break;
                    }
                    unwrapped_type = &unwrapped_type[8..]
                }
                let mut prefix = segments.join("::");
                if !prefix.is_empty() {
                    prefix += "::"
                }

                result.ref_ty = RefType::Deref(format!("[{}{}]", prefix, unwrapped_type));
                result.override_ty = Some(format!("::std::vec::Vec<{}{}>", prefix, unwrapped_type));
            }
            FieldKind::Bytes => {
                result.ref_ty = RefType::Deref("[u8]".to_owned());
                result.mt = MethodKind::Standard;
                result.take = Some(format!(
                    "::std::mem::replace(&mut self.{}, Default::default())",
                    result.name
                ));
            }
            FieldKind::String => {
                result.ref_ty = RefType::Deref("str".to_owned());
                result.mt = MethodKind::Standard;
                result.take = Some(format!(
                    "::std::mem::replace(&mut self.{}, Default::default())",
                    result.name
                ));
            }
            FieldKind::Enumeration(enum_type) => {
                result.override_ty = Some(enum_type.clone());
                result.ref_ty = RefType::Copy;
                result.clear = Some("0".to_owned());
                result.set = Some("v as i32".to_owned());
                result.enum_set = true;
                result.prost_has_unprefixed = true;
                result.get = Some(format!(
                    "match <{} as ::std::convert::TryFrom<i32>>::try_from(self.{}) {{\
                        Ok(e) => e,
                        Err(_) => panic!(\"Unknown enum variant: {{}}\", self.{1}),
                    }}",
                    type_in_expr_context(enum_type),
                    result.name,
                ));
            }
            FieldKind::Map => {
                result.mt = MethodKind::Standard;
            }
            // There's only a few `oneof`s and they are a bit complex, so easier to
            // handle manually.
            FieldKind::OneOf(_) => return None,
        }

        Some(result)
    }
}

fn unwrap_type(ty: &Type, type_ctor: &str) -> Type {
    match ty {
        Type::Path(p) => {
            let seg = p.path.segments.iter().last().unwrap();
            if seg.ident == type_ctor {
                match &seg.arguments {
                    PathArguments::AngleBracketed(args) => match &args.args[0] {
                        GenericArgument::Type(ty) => ty.clone(),
                        _ => unreachable!(),
                    },
                    _ => unreachable!(),
                }
            } else {
                ty.clone()
            }
        }
        _ => unreachable!(),
    }
}

struct FieldMethods {
    ty: String,
    ref_ty: RefType,
    override_ty: Option<String>,
    name: Ident,
    unesc_base: String,
    has: bool,
    // None = delegate to field's `clear`
    // Some = default value
    clear: Option<String>,
    // None = set to `v`
    // Some = expression to set.
    set: Option<String>,
    enum_set: bool,
    // Some = custom getter expression.
    get: Option<String>,
    mt: MethodKind,
    take: Option<String>,
    deprecated: bool,
    /// prost-derive 0.11 generates un-prefixed methods for bare (non-Option) enum fields
    prost_has_unprefixed: bool,
}

impl FieldMethods {
    fn new(ty: &Type, ident: &Ident, deprecated: bool) -> FieldMethods {
        let mut unesc_base = ident.to_string();
        if unesc_base.starts_with("r#") {
            unesc_base = unesc_base[2..].to_owned();
        }
        FieldMethods {
            ty: ty.clone().into_token_stream().to_string(),
            ref_ty: RefType::Ref,
            override_ty: None,
            name: ident.clone(),
            unesc_base,
            has: false,
            clear: None,
            set: None,
            enum_set: false,
            get: None,
            mt: MethodKind::None,
            take: None,
            deprecated,
            prost_has_unprefixed: false,
        }
    }

    fn write_methods<W>(&self, buf: &mut W, gen_opt: GenOpt) -> Result<(), io::Error>
    where
        W: Write,
    {
        let deprecated = if self.deprecated {
            "#[allow(deprecated)] "
        } else {
            ""
        };
        // has_*
        if self.has && gen_opt.contains(GenOpt::HAS) {
            writeln!(
                buf,
                "{}#[inline] pub fn has_{}(&self) -> bool {{ self.{}.is_some() }}",
                deprecated, self.unesc_base, self.name
            )?;
        }
        let ty = match &self.override_ty {
            Some(s) => s.clone(),
            None => self.ty.clone(),
        };
        let ref_ty = match &self.ref_ty {
            RefType::Copy => ty.clone(),
            RefType::Ref => format!("&{}", ty),
            RefType::Deref(s) => format!("&{}", s),
        };
        // clear_*
        if gen_opt.contains(GenOpt::CLEAR) {
            match &self.clear {
                Some(s) => writeln!(
                    buf,
                    "{}#[inline] pub fn clear_{}(&mut self) {{ self.{} = {} }}",
                    deprecated, self.unesc_base, self.name, s
                )?,
                None => writeln!(
                    buf,
                    "{}#[inline] pub fn clear_{}(&mut self) {{ self.{}.clear(); }}",
                    deprecated, self.unesc_base, self.name
                )?,
            }
        }
        // set_*
        match &self.set {
            Some(s) if !self.enum_set => writeln!(
                buf,
                "{}#[inline] pub fn set_{}(&mut self, v: {}) {{ self.{} = {}; }}",
                deprecated, self.unesc_base, ty, self.name, s
            )?,
            None if gen_opt.contains(GenOpt::TRIVIAL_SET) => writeln!(
                buf,
                "{}#[inline] pub fn set_{}(&mut self, v: {}) {{ self.{} = v; }}",
                deprecated, self.unesc_base, ty, self.name
            )?,
            _ => {}
        }
        // get_*
        match &self.get {
            Some(s) => {
                writeln!(
                    buf,
                    "{}#[inline] pub fn get_{}(&self) -> {} {{ {} }}",
                    deprecated, self.unesc_base, ref_ty, s
                )?;
                // Also generate an un-prefixed accessor for compatibility with protobuf 3.x
                // but only if prost-derive doesn't already generate one (e.g., for enum fields)
                if !self.prost_has_unprefixed {
                    writeln!(
                        buf,
                        "{}#[inline] pub fn {}(&self) -> {} {{ self.get_{}() }}",
                        deprecated, self.unesc_base, ref_ty, self.unesc_base
                    )?;
                }
            }
            None => {
                if gen_opt.contains(GenOpt::TRIVIAL_GET) {
                    let rf = match &self.ref_ty {
                        RefType::Copy => "",
                        _ => "&",
                    };
                    writeln!(
                        buf,
                        "{}#[inline] pub fn get_{}(&self) -> {} {{ {}self.{} }}",
                        deprecated, self.unesc_base, ref_ty, rf, self.name
                    )?;
                    // Also generate an un-prefixed accessor for compatibility with protobuf 3.x
                    if !self.prost_has_unprefixed {
                        writeln!(
                            buf,
                            "{}#[inline] pub fn {}(&self) -> {} {{ self.get_{}() }}",
                            deprecated, self.unesc_base, ref_ty, self.unesc_base
                        )?;
                    }
                }
            }
        }
        // mut_*
        if gen_opt.contains(GenOpt::MUT) {
            match &self.mt {
                MethodKind::Standard => {
                    writeln!(
                        buf,
                        "{}#[inline] pub fn mut_{}(&mut self) -> &mut {} {{ &mut self.{} }}",
                        deprecated, self.unesc_base, ty, self.name
                    )?;
                }
                MethodKind::Custom(s) => {
                    writeln!(
                        buf,
                        "{}#[inline] pub fn mut_{}(&mut self) -> &mut {} {{ {} }} ",
                        deprecated, self.unesc_base, ty, s
                    )?;
                }
                MethodKind::None => {}
            }
        }

        // take_*
        if gen_opt.contains(GenOpt::TAKE) {
            if let Some(s) = &self.take {
                writeln!(
                    buf,
                    "{}#[inline] pub fn take_{}(&mut self) -> {} {{ {} }}",
                    deprecated, self.unesc_base, ty, s
                )?;
            }
        }

        Ok(())
    }
}

enum RefType {
    Copy,
    Ref,
    Deref(String),
}

enum MethodKind {
    None,
    Standard,
    Custom(String),
}

fn is_message(attrs: &[Attribute]) -> bool {
    for a in attrs {
        if a.path.is_ident("derive") {
            let tts = a.tokens.to_string();
            if tts.contains(":: Message") {
                return true;
            }
        }
    }
    false
}

fn is_enum(attrs: &[Attribute]) -> bool {
    for a in attrs {
        if a.path.is_ident("derive") {
            let tts = a.tokens.to_string();
            if tts.contains("Enumeration") {
                return true;
            }
        }
    }
    false
}

// When a generic type is used in expression context, it might need to be adjusted.
// For example, `Box<Foo>` becomes `Box::<Foo>`
fn type_in_expr_context(s: &str) -> String {
    let mut parsed: TypePath = syn::parse_str(s).expect("Not a type?");
    let last_segment = parsed.path.segments.last_mut().unwrap();
    if !last_segment.arguments.is_empty() {
        if let PathArguments::AngleBracketed(ref mut a) = last_segment.arguments {
            if a.colon2_token.is_none() {
                a.colon2_token = Some(Token![::](Span::call_site()));
            }
        }
    }
    parsed.to_token_stream().to_string()
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_type_in_expr_context() {
        assert_eq!("T", type_in_expr_context("T"));
        assert_eq!(
            ":: foo :: bar :: Vec :: < Baz >",
            type_in_expr_context("::foo::bar::Vec<Baz>")
        );
        assert_eq!(
            ":: foo :: bar :: Vec :: < Box < Baz > >",
            type_in_expr_context("::foo::bar::Vec::<Box<Baz>>")
        );
        assert_eq!(
            ":: foo :: bar :: Vec :: < Box < Baz > >",
            type_in_expr_context("::foo::bar::Vec<Box<Baz>>")
        );
    }
}
