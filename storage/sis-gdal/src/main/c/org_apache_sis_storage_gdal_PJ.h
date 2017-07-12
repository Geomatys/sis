/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_apache_sis_storage_gdal_PJ */

#ifndef _Included_org_apache_sis_storage_gdal_PJ
#define _Included_org_apache_sis_storage_gdal_PJ
#ifdef __cplusplus
extern "C" {
#endif
#undef org_apache_sis_storage_gdal_PJ_DIMENSION_MAX
#define org_apache_sis_storage_gdal_PJ_DIMENSION_MAX 100L
/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    allocatePJ
 * Signature: (Ljava/lang/String;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_sis_storage_gdal_PJ_allocatePJ
  (JNIEnv *, jclass, jstring);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    allocateGeoPJ
 * Signature: (Lorg/apache/sis/storage/gdal/PJ;)J
 */
JNIEXPORT jlong JNICALL Java_org_apache_sis_storage_gdal_PJ_allocateGeoPJ
  (JNIEnv *, jclass, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getVersion
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_sis_storage_gdal_PJ_getVersion
  (JNIEnv *, jclass);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getDefinition
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_sis_storage_gdal_PJ_getDefinition
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getType
 * Signature: ()Lorg/apache/sis/storage/gdal/PJ/Type;
 */
JNIEXPORT jobject JNICALL Java_org_apache_sis_storage_gdal_PJ_getType
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getEccentricitySquared
 * Signature: ()D
 */
JNIEXPORT jdouble JNICALL Java_org_apache_sis_storage_gdal_PJ_getEccentricitySquared
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getSemiMajorAxis
 * Signature: ()D
 */
JNIEXPORT jdouble JNICALL Java_org_apache_sis_storage_gdal_PJ_getSemiMajorAxis
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getSemiMinorAxis
 * Signature: ()D
 */
JNIEXPORT jdouble JNICALL Java_org_apache_sis_storage_gdal_PJ_getSemiMinorAxis
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getGreenwichLongitude
 * Signature: ()D
 */
JNIEXPORT jdouble JNICALL Java_org_apache_sis_storage_gdal_PJ_getGreenwichLongitude
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getAxisDirections
 * Signature: ()[C
 */
JNIEXPORT jcharArray JNICALL Java_org_apache_sis_storage_gdal_PJ_getAxisDirections
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getLinearUnitToMetre
 * Signature: (Z)D
 */
JNIEXPORT jdouble JNICALL Java_org_apache_sis_storage_gdal_PJ_getLinearUnitToMetre
  (JNIEnv *, jobject, jboolean);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    transform
 * Signature: (Lorg/apache/sis/storage/gdal/PJ;I[DII)V
 */
JNIEXPORT void JNICALL Java_org_apache_sis_storage_gdal_PJ_transform
  (JNIEnv *, jobject, jobject, jint, jdoubleArray, jint, jint);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    getLastError
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_sis_storage_gdal_PJ_getLastError
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    toString
 * Signature: ()Ljava/lang/String;
 */
JNIEXPORT jstring JNICALL Java_org_apache_sis_storage_gdal_PJ_toString
  (JNIEnv *, jobject);

/*
 * Class:     org_apache_sis_storage_gdal_PJ
 * Method:    finalize
 * Signature: ()V
 */
JNIEXPORT void JNICALL Java_org_apache_sis_storage_gdal_PJ_finalize
  (JNIEnv *, jobject);

#ifdef __cplusplus
}
#endif
#endif
