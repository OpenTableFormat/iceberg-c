#pragma once

#ifndef ICEBERG_EXPORT
#define ICEBERG_EXPORT [[gnu::visibility("default")]]
#endif
#ifndef ICEBERG_NO_EXPORT
#define ICEBERG_NO_EXPORT [[gnu::visibility("hidden")]]
#endif
